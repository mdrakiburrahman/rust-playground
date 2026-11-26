import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { Bool, Field, Int32, Int64, List, Struct, Table, Utf8, tableToIPC, vectorFromArray } from 'apache-arrow';
import { Table as WasmTable, writeParquet } from 'parquet-wasm';
import { HttpFailure, type Azure, type HttpReceipt } from '../../infra/azure/bootstrap.js';
import {
  DELTA_READ_CONCURRENCY, parseDeltaReplayReference, readDeltaAddWindow, readDeltaReference, verifyDeltaPreservation,
  type DeltaReplayReference, type DeltaRetentionPolicy,
} from './aks-delta-preservation.js';

type Json = Record<string, any>;
const root = 'planned/hot-table', prefix = root + '/_delta_log/';
const guard = async () => {};

test('Delta history reads are capped and replayed in version order despite out-of-order responses', async () => {
  const s = new Storage().put(0, { protocol: legacyProtocol }, { metaData: meta() }, info('CREATE TABLE'));
  for (let version = 1; version <= DELTA_READ_CONCURRENCY * 2; version++)
    s.put(version, info('WRITE'), add(`part-${version}.parquet`));
  const original = s.azure.blob.bind(s.azure);
  let active = 0, peak = 0;
  s.azure.blob = async (...args: Parameters<Azure['blob']>) => {
    if (args[3]) return original(...args);
    active++; peak = Math.max(peak, active);
    try {
      await new Promise<void>(resolve => setTimeout(resolve, Number(args[2]?.slice(-6, -5)) % 2 ? 1 : 5));
      return await original(...args);
    } finally { active--; }
  };
  const value = await readDeltaReference(s.azure, root, guard);
  assert.equal(value.observation, 'table');
  if (value.observation !== 'table') assert.fail('actual table required');
  assert.equal(value.row_count, DELTA_READ_CONCURRENCY * 2 * 10);
  assert.equal(value.version, DELTA_READ_CONCURRENCY * 2);
  assert.ok(peak > 1);
  assert.ok(peak <= DELTA_READ_CONCURRENCY);
});

test('one Delta deadline spans every batch; expiry never starts a fresh batch or returns measured rows', async t => {
  const s = new Storage().put(0, { protocol: legacyProtocol }, { metaData: meta() }, info('CREATE TABLE'));
  for (let version = 1; version <= DELTA_READ_CONCURRENCY * 4; version++)
    s.put(version, info('WRITE'), add(`part-${version}.parquet`));
  let now = 1_000_000;
  t.mock.method(Date, 'now', () => now);
  s.beforeGet = () => { now += 10; };
  await assert.rejects(readDeltaReference(s.azure, root, guard, now + DELTA_READ_CONCURRENCY * 10 + 1),
    /Delta read shared deadline expired/);
  assert.equal(s.calls.filter(call => !call.query).length, DELTA_READ_CONCURRENCY + 1);
  assert.equal(s.listCount, 1, 'expired replay must not produce a final positive listing');
});

test('a hung Delta GET expires at the shared deadline without manufacturing absence', async () => {
  let calls = 0;
  const azure = { c: { account: 'arrowcollector00000', data: 'data', table_prefix: 'planned' },
    blob: async () => { calls++; return new Promise<never>(() => {}); } } as unknown as Azure;
  await assert.rejects(readDeltaReference(azure, root, guard, Date.now() + 30), /Delta read shared deadline expired/);
  assert.equal(calls, 1);
});

test('Delta deadline expiry is typed unknown preservation, never a zero-row or absent snapshot', async () => {
  const s = new Storage().put(0, { protocol: legacyProtocol }, { metaData: meta() }, info('CREATE TABLE'), add('part.parquet'));
  const before = await readDeltaReference(s.azure, root, guard);
  s.azure.blob = async () => new Promise<never>(() => {});
  const result = await verifyDeltaPreservation(s.azure, before, before, guard, undefined, Date.now() + 30);
  assert.equal(result.status, 'unknown');
  assert.equal(result.current, null);
  assert.match(result.reason!, /Delta read shared deadline expired/);
});
const meta = (id = '11111111-1111-4111-8111-111111111111') => ({ id, format: { provider: 'parquet' },
  schemaString: '{"type":"struct","fields":[{"name":"__arrowcol_ingestion","type":"string","nullable":true,"metadata":{}}]}',
  partitionColumns: ['__arrowcol_ingestion'], configuration: { 'delta.enableDeletionVectors': 'false' } });
const legacyProtocol = { minReaderVersion: 1, minWriterVersion: 2 };
const add = (path: string, records: number | null = 10, dataChange = true, partition = '2026-11-15') => ({
  add: { path, size: 100, dataChange, partitionValues: { __arrowcol_ingestion: partition },
    ...(records === null ? {} : { stats: JSON.stringify({ numRecords: records }) }) },
});
const remove = (path: string, dataChange = true) => ({ remove: { path, dataChange, deletionTimestamp: Date.parse('2026-11-16') } });
const info = (operation: string, extra: Json = {}) => ({ commitInfo: { operation, timestamp: Date.parse('2026-11-16'),
  ...(operation === 'OPTIMIZE' || operation === 'DELETE' ? { engineInfo: 'arrow-deltalake-maintenance' } : {}),
  ...extra } });
const xml = (value: string) => value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('"', '&quot;');
const digest = (bytes: Buffer) => createHash('sha256').update(bytes).digest('hex');
class Storage {
  objects = new Map<string, Buffer>();
  calls: Array<{ name: string; query?: Record<string, string>; headers: Record<string, string> }> = [];
  listCount = 0;
  status = 200;
  omitReceipt = false;
  responseEtag: string | null = null;
  pageSize = 1000;
  omitRequestId = false;
  repeatRequestId = false;
  listingPrefix = prefix;
  listingComplete = true;
  beforeList?: (number: number) => void;
  beforeGet?: (name: string) => void;
  azure = { c: { account: 'arrowcollector00000', data: 'data', table_prefix: 'planned' },
    blob: async (method: string, container: string, name = '', query?: Record<string, string>, body?: unknown,
      headers: Record<string, string> = {}, missingOk?: boolean, idempotent?: boolean, capture?: (r: HttpReceipt) => void) => {
      assert.equal(method, 'GET', 'replay must never mutate storage');
      assert.equal(container, 'data');
      assert.equal(body, undefined);
      assert.equal(missingOk, false);
      assert.equal(idempotent, true);
      this.calls.push({ name, query, headers });
      if (!this.omitReceipt) capture?.({ status: this.status, bodyBase64: '', headers: {} });
      if (query) {
        assert.deepEqual({ ...query, marker: '' }, { restype: 'container', comp: 'list', prefix, marker: '', maxresults: '1000' });
        this.listCount++;
        this.beforeList?.(this.listCount);
        const all = [...this.objects].filter(([key]) => key.startsWith(prefix)).sort(([a], [b]) => a.localeCompare(b));
        const start = Number(query.marker || '0'), entries = all.slice(start, start + this.pageSize);
        const next = start + entries.length < all.length ? String(start + entries.length) : '';
        return [Buffer.from(`<EnumerationResults><Prefix>${xml(this.listingPrefix)}</Prefix><Blobs>${entries.map(([key, bytes]) =>
          `<Blob><Name>${xml(key)}</Name><Properties><Etag>${xml('0x' + digest(bytes).slice(0, 15))}</Etag>` +
          `<Content-Length>${bytes.length}</Content-Length></Properties></Blob>`).join('')}</Blobs>` +
          (this.listingComplete ? `<NextMarker>${next}</NextMarker>` : '') + '</EnumerationResults>'),
        this.omitRequestId ? {} : { 'x-ms-request-id': this.repeatRequestId ? 'same-request-id' : `request-${this.listCount}` }];
      }
      this.beforeGet?.(name);
      assert.ok(name.startsWith(prefix));
      const bytes = this.objects.get(name);
      assert.ok(bytes, 'required object must exist');
      const etag = '"0x' + digest(bytes).slice(0, 15) + '"';
      assert.equal(headers['If-Match'], etag, 'Delta reads must pin the listed strong ETag');
      return [bytes, { etag: this.responseEtag ?? etag }];
    } } as unknown as Azure;
  put(version: number, ...actions: Json[]): this {
    this.objects.set(prefix + version.toString().padStart(20, '0') + '.json', Buffer.from(actions.map(a => JSON.stringify(a)).join('\n') + '\n'));
    return this;
  }
  seed(...actions: Json[]): this {
    return this.put(0, { protocol: legacyProtocol }, { metaData: meta() }, info('CREATE TABLE'), ...actions);
  }
  observe() { return readDeltaReference(this.azure, root, guard); }
  async read() {
    const reference = await this.observe();
    assert.ok(reference.observation === 'table');
    return reference;
  }
}
function checkpoint(actions: Json[]): Buffer {
  const addType = new Struct([new Field('path', new Utf8(), true), new Field('size', new Int64(), true),
    new Field('dataChange', new Bool(), true),
    new Field('partitionValues', new Struct([new Field('__arrowcol_ingestion', new Utf8(), true)]), true),
    new Field('stats', new Utf8(), true)]);
  const metaType = new Struct([new Field('id', new Utf8(), true),
    new Field('format', new Struct([new Field('provider', new Utf8(), true)]), true),
    new Field('schemaString', new Utf8(), true), new Field('partitionColumns', new List(new Field('item', new Utf8(), true)), true),
    new Field('configuration', new Struct([new Field('delta.enableDeletionVectors', new Utf8(), true)]), true)]);
  const protocolType = new Struct([new Field('minReaderVersion', new Int32(), true), new Field('minWriterVersion', new Int32(), true)]);
  const table = new Table({
    add: vectorFromArray(actions.map(a => a.add ? { ...a.add, size: BigInt(a.add.size) } : null), addType),
    metaData: vectorFromArray(actions.map(a => a.metaData ?? null), metaType),
    protocol: vectorFromArray(actions.map(a => a.protocol ?? null), protocolType),
  });
  return Buffer.from(writeParquet(WasmTable.fromIPCStream(tableToIPC(table, 'stream'))));
}
const result = async (storage: Storage, baseline: DeltaReplayReference,
  terminal = baseline, retention?: DeltaRetentionPolicy) =>
  verifyDeltaPreservation(storage.azure, baseline, terminal, guard, retention);

test('reader accepts Delta protocol with only the invariants writer feature', async () => {
  const protocol = { minReaderVersion: 3, minWriterVersion: 7, readerFeatures: [], writerFeatures: ['invariants'] };
  const s = new Storage().put(0, { protocol }, { metaData: meta() }, info('CREATE TABLE'), add('seed.parquet', 17));
  const reference = await s.read();
  assert.equal(reference.row_count, 17);
  assert.deepEqual(reference.protocol, protocol);
});

test('Delta Add window retries one append that races its initial snapshot', async () => {
  const s = new Storage().seed(add('seed.parquet', 10));
  s.put(1, info('INSERT'), add('first.parquet', 20));
  s.beforeList = count => { if (count === 2) s.put(2, info('INSERT'), add('racing.parquet', 30)); };
  const result = await readDeltaAddWindow(s.azure, root, 0, Date.parse('2026-11-17'), guard);
  assert.equal(result.total_rows, 60);
  assert.equal(result.window_rows, 60);
  assert.equal(result.window_commits, 3);
  assert.equal(result.active_rows, 60);
});

test('QAP source evidence retains the exact active files and refuses to label a physical rewrite append-only', async () => {
  const s = new Storage().seed(add('seed.parquet', 10));
  s.put(1, info('INSERT'), add('ingested.parquet', 20));
  const append = await readDeltaAddWindow(s.azure, root, 0, Date.parse('2026-11-17'), guard);
  assert.equal(append.append_only, true);
  assert.deepEqual(append.source_reference.active_files.map(file => [file.path, file.num_records]),
    [['ingested.parquet', 20], ['seed.parquet', 10]]);
  s.put(2, info('OPTIMIZE'), remove('seed.parquet', false), remove('ingested.parquet', false),
    add('compact.parquet', 30, false));
  const rewrite = await readDeltaAddWindow(s.azure, root, 0, Date.parse('2026-11-17'), guard);
  assert.equal(rewrite.active_rows, 30);
  assert.equal(rewrite.append_only, false);
  assert.deepEqual(rewrite.source_reference.active_files.map(file => file.path), ['compact.parquet']);
});

test('Delta Add window retries a conditional GET that loses to a concurrent checkpoint rewrite', async () => {
  const s = new Storage().seed(add('seed.parquet', 10));
  s.put(1, info('INSERT'), add('first.parquet', 20));
  const pointer = prefix + '_last_checkpoint';
  s.objects.set(prefix + '00000000000000000000.checkpoint.parquet',
    checkpoint([{ protocol: legacyProtocol }, { metaData: meta() }, add('seed.parquet', 10)]));
  s.objects.set(pointer, Buffer.from('{"version":0,"size":3}'));
  let pointerReads = 0;
  s.beforeGet = name => {
    if (name === pointer && ++pointerReads === 1) {
      s.objects.set(pointer, Buffer.from('{"version":0,"size":3}\n'));
      throw new HttpFailure(412, 'HTTP 412: condition not met');
    }
  };
  const result = await readDeltaAddWindow(s.azure, root, 0, Date.parse('2026-11-17'), guard);
  assert.equal(pointerReads, 2);
  assert.equal(result.window_rows, 30);
  let immutableReads = 0;
  s.beforeGet = name => {
    if (name === prefix + '00000000000000000001.json' && ++immutableReads === 1)
      throw new HttpFailure(412, 'immutable commit changed');
  };
  await assert.rejects(readDeltaAddWindow(s.azure, root, 0, Date.parse('2026-11-17'), guard), /immutable commit changed/);
  assert.equal(immutableReads, 1);
});

test('fresh replay preserves explicit zero only for measured empty active-file sets; references round trip strictly', async () => {
  const s = new Storage().seed(), reference = await s.read();
  assert.equal(reference.row_count, 0);
  assert.equal(reference.active_file_count, 0);
  assert.equal(reference.active_bytes, 0);
  assert.deepEqual(parseDeltaReplayReference(JSON.parse(JSON.stringify(reference)), s.azure, root), reference);
  assert.equal((await result(s, reference)).status, 'preserved');
  for (const tampered of [
    { ...reference, active_bytes: 1 }, { ...reference, table_id: 'changed' },
    { ...reference, account: 'anotheraccount' }, { ...reference, unexpected: 'field' },
    { ...reference, provenance: { ...reference.provenance, source_objects: [] } },
  ]) assert.throws(() => parseDeltaReplayReference(tampered, s.azure, root));
});

test('ingestion append and dataChange=false OPTIMIZE preserve rows committed after baseline', async () => {
  const s = new Storage().seed(add('seed.parquet', 7)), baseline = await s.read();
  s.put(1, info('INSERT'), add('ingested.parquet', 13));
  const terminal = await s.read();
  s.put(2, info('OPTIMIZE'), remove('seed.parquet', false), remove('ingested.parquet', false), add('compact.parquet', 20, false));
  const verified = await result(s, baseline, terminal);
  assert.equal(verified.status, 'preserved', verified.reason ?? '');
  assert.equal(verified.current?.row_count, 20);
  assert.equal(verified.purge_explained_rows, 0);
  assert.equal(verified.current?.active_file_count, 1);
});

test('unexplained logical removals cannot hide behind an unchanged baseline count', async () => {
  const s = new Storage().seed(add('seed.parquet', 10)), baseline = await s.read();
  s.put(1, info('INSERT'), add('during-cell.parquet', 30));
  const terminal = await s.read();
  s.put(2, info('DELETE', { engineInfo: 'other-writer' }), remove('during-cell.parquet'));
  const verified = await result(s, baseline, terminal);
  assert.equal(verified.status, 'failed');
  assert.match(verified.reason!, /unexplained/);
  assert.equal(verified.current?.row_count, 10);
  assert.equal(verified.purge_explained_rows, null);
});

test('same ID and nondecreasing versions are required for terminal and current references', async () => {
  const s = new Storage().seed(add('seed.parquet')), baseline = await s.read();
  s.put(1, info('INSERT'), add('later.parquet'));
  const terminal = await s.read();
  assert.equal((await result(s, terminal, baseline)).status, 'failed');
  s.objects.delete(prefix + '00000000000000000001.json');
  assert.equal((await result(s, baseline, terminal)).status, 'failed');
  s.put(0, { protocol: legacyProtocol }, { metaData: meta('replacement-table') }, info('CREATE TABLE'), add('replacement.parquet'));
  const replacement = await s.read();
  assert.equal((await result(s, baseline, replacement)).status, 'failed');
  assert.equal((await result(s, baseline)).status, 'failed');
});

test('same-version terminal manifests and immutable history cannot be rewritten', async () => {
  const s = new Storage().seed(add('a.parquet')), baseline = await s.read();
  s.put(0, { protocol: legacyProtocol }, { metaData: meta() }, info('CREATE TABLE'), add('b.parquet'));
  const different = await s.read();
  const verified = await result(s, baseline, different);
  assert.equal(verified.status, 'failed');
  assert.match(verified.reason!, /immutable|same Delta version/);
});

test('missing file stats remain null and cannot yield PASS, including data compacted before the final replay', async () => {
  const s = new Storage().seed(add('unknown.parquet', null)), unknown = await s.read();
  assert.equal(unknown.row_count, null);
  assert.equal(unknown.active_files[0].num_records, null);
  assert.equal((await result(s, unknown)).status, 'unknown');
  const good = new Storage().seed(add('seed.parquet')), baseline = await good.read();
  good.put(1, info('INSERT'), add('no-stats.parquet', null));
  good.put(2, info('OPTIMIZE'), remove('seed.parquet', false), remove('no-stats.parquet', false), add('compact.parquet', 20, false));
  const terminal = await good.read();
  assert.equal(terminal.row_count, 20);
  assert.equal((await result(good, baseline, terminal)).status, 'unknown');
});

test('standard single and multipart Parquet checkpoints replay with contiguous following JSON commits', async () => {
  for (const multipart of [false, true]) {
    const s = new Storage();
    const parts = multipart ? [[{ protocol: legacyProtocol }, { metaData: meta() }], [add('checkpoint.parquet', 8)]] :
      [[{ protocol: legacyProtocol }, { metaData: meta() }, add('checkpoint.parquet', 8)]];
    for (let index = 0; index < parts.length; index++) s.objects.set(prefix + '00000000000000000004.checkpoint' +
      (multipart ? `.${String(index + 1).padStart(10, '0')}.${String(parts.length).padStart(10, '0')}` : '') + '.parquet',
    checkpoint(parts[index]));
    s.objects.set(prefix + '_last_checkpoint', Buffer.from(JSON.stringify({ version: 4, size: 3, ...(multipart ? { parts: 2 } : {}) })));
    s.put(5, info('INSERT'), add('after-checkpoint.parquet', 12));
    const baseline = await s.read();
    assert.equal(baseline.row_count, 20);
    assert.equal(baseline.provenance.checkpoint_version, 4);
    assert.deepEqual(parseDeltaReplayReference(baseline, s.azure, root), baseline);
    s.put(6, info('INSERT'), add('after-baseline.parquet', 3));
    const verified = await result(s, baseline);
    assert.equal(verified.status, 'preserved', verified.reason ?? '');
    assert.equal(verified.current?.row_count, 23);
  }
});

test('checkpoint recovery is not proof of missing post-baseline commit history', async () => {
  const s = new Storage().seed(add('seed.parquet')), baseline = await s.read();
  s.objects.clear();
  s.objects.set(prefix + '00000000000000000002.checkpoint.parquet',
    checkpoint([{ protocol: legacyProtocol }, { metaData: meta() }, add('seed.parquet'), add('new.parquet')]));
  const terminal = await s.read();
  assert.equal(terminal.row_count, 20);
  const verified = await result(s, baseline, terminal);
  assert.equal(verified.status, 'unknown');
  assert.match(verified.reason!, /history/);
});

test('missing genesis, JSON gaps, incomplete checkpoints and v2/sidecar formats fail closed', async () => {
  const missing = new Storage().put(1, info('INSERT'), add('late.parquet'));
  await assert.rejects(missing.read(), /history/);
  const gap = new Storage().seed(add('a.parquet')).put(2, info('INSERT'), add('b.parquet'));
  await assert.rejects(gap.read(), /history/);
  const incomplete = new Storage();
  incomplete.objects.set(prefix + '00000000000000000004.checkpoint.0000000001.0000000002.parquet',
    checkpoint([{ protocol: legacyProtocol }, { metaData: meta() }]));
  await assert.rejects(incomplete.read(), /multipart/);
  for (const name of ['00000000000000000001.checkpoint.abc.json', '_sidecars/part.parquet']) {
    const s = new Storage().seed();
    s.objects.set(prefix + name, Buffer.from('{}'));
    await assert.rejects(s.read(), /unsupported/);
  }
});

test('changed GET ETags, listing races, missing receipts and nonpositive statuses never produce references', async () => {
  const etag = new Storage().seed();
  etag.responseEtag = '"different"';
  await assert.rejects(etag.read(), /ETag/);
  const weak = new Storage().seed();
  weak.responseEtag = 'W/"weak"';
  await assert.rejects(weak.read(), /ETag/);
  const race = new Storage().seed();
  race.beforeList = count => { if (count === 2) race.put(1, info('INSERT'), add('racing.parquet')); };
  await assert.rejects(race.read(), /listing changed/);
  const rewrite = new Storage().seed();
  rewrite.beforeList = count => { if (count === 2) rewrite.seed(add('rewritten.parquet')); };
  await assert.rejects(rewrite.read(), /listing changed/);
  const noReceipt = new Storage().seed();
  noReceipt.omitReceipt = true;
  await assert.rejects(noReceipt.read(), /positive/);
  const failed = new Storage().seed();
  failed.status = 404;
  await assert.rejects(failed.read(), /positive/);
});

test('prefix listing is bounded, paginated, read-only and guarded around every request', async () => {
  const s = new Storage().seed(add('seed.parquet')).put(1, info('INSERT'), add('next.parquet'));
  s.pageSize = 1;
  let guards = 0;
  await readDeltaReference(s.azure, root, async () => { guards++; });
  assert.equal(guards, 2 * s.calls.length);
  assert.ok(s.calls.every(call => call.headers.Range?.startsWith('bytes=0-')));
  const cancel = new Storage().seed();
  await assert.rejects(readDeltaReference(cancel.azure, root, async () => { throw new Error('lease lost'); }), /lease lost/);
  assert.equal(cancel.calls.length, 0);
});

test('URI/SAS components, traversal, escapes, ambiguous paths and another prefix are rejected before storage reads', async () => {
  for (const path of ['../table', '/table', 'planned//table', 'planned/../table', 'planned/%2e%2e/table',
    'planned/table?sig=secret', 'abfss://data@account/table', 'planned\\table', 'planned/table#fragment', 'elsewhere/table']) {
    const s = new Storage().seed();
    await assert.rejects(readDeltaReference(s.azure, path, guard));
    assert.equal(s.calls.length, 0, path);
  }
  const s = new Storage().seed(add('../escaped.parquet'));
  await assert.rejects(s.read(), /unsafe relative path/);
});

test('unsupported deletion vectors, protocol features, unknown actions and unsafe integers are UNKNOWN', async () => {
  const cases: Json[][] = [
    [{ protocol: { minReaderVersion: 3, minWriterVersion: 7, readerFeatures: ['deletionVectors'] } }, { metaData: meta() }],
    [{ protocol: legacyProtocol }, { metaData: meta() }, { add: { ...add('dv.parquet').add, deletionVector: { storageType: 'i' } } }],
    [{ protocol: legacyProtocol }, { metaData: meta() }, { domainMetadata: { domain: 'unknown' } }],
    [{ protocol: legacyProtocol }, { metaData: meta() }, { add: { ...add('huge.parquet').add, size: Number.MAX_SAFE_INTEGER + 1 } }],
  ];
  for (const actions of cases) await assert.rejects(new Storage().put(0, ...actions).read());
});

test('OPTIMIZE must be false-dataChange maintenance with equal measured input and output rows', async () => {
  const s = new Storage().seed(add('seed.parquet', 20)), baseline = await s.read();
  s.put(1, info('OPTIMIZE'), remove('seed.parquet', false), add('lost.parquet', 10, false));
  assert.equal((await result(s, baseline)).status, 'failed');
  s.put(1, info('OPTIMIZE', { engineInfo: 'unknown' }), remove('seed.parquet', false), add('valid.parquet', 20, false));
  assert.equal((await result(s, baseline)).status, 'unknown');
});

test('a maintenance DELETE watermark alone is not configured-retention proof', async () => {
  const s = new Storage().seed(add('old.parquet', 7, true, '2026-11-10'), add('new.parquet', 11)), baseline = await s.read();
  s.put(1, info('DELETE'), { txn: { appId: 'arrowcol.purge.before-ms', version: Date.parse('2026-11-11') } }, remove('old.parquet'));
  const verified = await result(s, baseline);
  assert.equal(verified.status, 'unknown');
  assert.match(verified.reason!, /configured retention/);
  assert.equal(verified.purge_explained_rows, null);
});

test('only whole expired partitions, a maintenance watermark and explicit trusted retention explain PURGE rows', async () => {
  const policy: DeltaRetentionPolicy = { retention_ms: 3 * 86_400_000, granularity: 'day' };
  const s = new Storage().seed(add('old-a.parquet', 7, true, '2026-11-10'),
    add('old-b.parquet', 5, true, '2026-11-10'), add('new.parquet', 11)), baseline = await s.read();
  const watermark = { txn: { appId: 'arrowcol.purge.before-ms', version: Date.parse('2026-11-11') } };
  s.put(1, info('DELETE'), watermark, remove('old-a.parquet'), remove('old-b.parquet'));
  const verified = await result(s, baseline, baseline, policy);
  assert.equal(verified.status, 'preserved', verified.reason ?? '');
  assert.equal(verified.purge_explained_rows, 12);
  assert.equal(verified.current?.row_count, 11);
  assert.equal((await result(s, baseline, baseline, { ...policy, retention_ms: 10 * 86_400_000 })).status, 'unknown');
  s.put(1, info('DELETE'), watermark, remove('old-a.parquet'));
  assert.equal((await result(s, baseline, baseline, policy)).status, 'unknown');
  s.put(1, info('DELETE'), remove('old-a.parquet'), remove('old-b.parquet'));
  assert.equal((await result(s, baseline, baseline, policy)).status, 'unknown');
});

test('terminal rows are independently protected even when a pre-terminal PURGE explained other rows', async () => {
  const s = new Storage().seed(add('old.parquet', 10, true, '2026-11-10'), add('new.parquet', 10)), baseline = await s.read();
  s.put(1, info('DELETE'), { txn: { appId: 'arrowcol.purge.before-ms', version: Date.parse('2026-11-11') } }, remove('old.parquet'));
  const terminal = await s.read();
  s.put(2, info('INSERT'), add('in-cell.parquet', 30));
  s.put(3, info('DELETE', { engineInfo: 'other' }), remove('in-cell.parquet'));
  assert.equal((await result(s, baseline, terminal, { retention_ms: 3 * 86_400_000, granularity: 'day' })).status, 'failed');
});

test('exact planned-root absence needs two fresh positive complete listings and never manufactures zero rows', async () => {
  const s = new Storage(), reference = await s.observe();
  assert.equal(reference.observation, 'observed_absent');
  assert.equal(reference.table_id, null);
  assert.equal(reference.version, null);
  assert.equal(reference.row_count, null);
  assert.equal(reference.active_file_count, null);
  assert.equal(reference.active_bytes, null);
  assert.equal(reference.active_files, null);
  assert.equal(s.calls.length, 2);
  assert.ok(s.calls.every(call => call.query?.prefix === prefix && call.headers['Cache-Control'] === 'no-cache'));
  assert.deepEqual(parseDeltaReplayReference(JSON.parse(JSON.stringify(reference)), s.azure, root), reference);
  const verified = await result(s, reference);
  assert.equal(verified.status, 'preserved');
  assert.equal(verified.disposition, 'absence_preserved');
  assert.equal(verified.current?.observation, 'observed_absent');
  assert.equal(verified.purge_explained_rows, null);
  assert.match(verified.reason!, /no row-delivery proof/);
});

test('absence parsing requires exact null fields and deeply bound positive provenance', async () => {
  const s = new Storage(), reference = await s.observe();
  assert.ok(reference.observation === 'observed_absent');
  for (const tampered of [
    { ...reference, row_count: 0 },
    { ...reference, version: 0 },
    { ...reference, active_files: [] },
    { ...reference, table_id: 'guessed' },
    { ...reference, provenance: { ...reference.provenance, observations: [] } },
    { ...reference, provenance: { ...reference.provenance, log_prefix: 'another/_delta_log/' } },
    { ...reference, provenance: { ...reference.provenance, observations:
      reference.provenance.observations.map(observation => ({ ...observation, status: 403 })) } },
    { ...reference, provenance: { ...reference.provenance, observations:
      reference.provenance.observations.map(observation => ({ ...observation, completed: false })) } },
    { ...reference, observed_at_unix_ms: 0 },
  ]) assert.throws(() => parseDeltaReplayReference(tampered, s.azure, root));
});

test('auth/permission failures, incomplete receipts, wrong prefixes and listing races cannot become absence', async () => {
  for (const status of [0, 204, 206, 401, 403, 404, 500]) {
    const s = new Storage();
    s.status = status;
    await assert.rejects(s.observe(), `status ${status}`);
  }
  for (const configure of [
    (s: Storage) => { s.omitReceipt = true; },
    (s: Storage) => { s.omitRequestId = true; },
    (s: Storage) => { s.repeatRequestId = true; },
    (s: Storage) => { s.listingComplete = false; },
    (s: Storage) => { s.listingPrefix = 'another/_delta_log/'; },
    (s: Storage) => { s.beforeList = count => { if (count === 2) s.seed(); }; },
    (s: Storage) => { s.beforeList = () => { throw new Error('network unavailable'); }; },
  ]) {
    const s = new Storage();
    configure(s);
    await assert.rejects(s.observe());
  }
  const partial = new Storage();
  partial.objects.set(prefix + '_last_checkpoint', Buffer.from('{"version":4,"size":1}'));
  await assert.rejects(partial.observe(), /incomplete/);
  partial.objects.clear();
  partial.objects.set(prefix + '00000000000000000001.json', Buffer.from(JSON.stringify(info('INSERT'))));
  await assert.rejects(partial.observe(), /history/);
});

test('absent preload to actual terminal replays creation, ingestion and final maintenance independently', async () => {
  const s = new Storage(), baseline = await s.observe();
  s.seed(add('created.parquet', 7));
  s.put(1, info('INSERT'), add('during-cell.parquet', 13));
  const terminal = await s.read();
  s.put(2, info('OPTIMIZE'), remove('created.parquet', false), remove('during-cell.parquet', false), add('compact.parquet', 20, false));
  const verified = await result(s, baseline, terminal);
  assert.equal(verified.status, 'preserved', verified.reason ?? '');
  assert.equal(verified.disposition, 'table_preserved');
  assert.equal(verified.current?.row_count, 20);
  assert.equal(verified.current?.table_id, terminal.table_id);
  assert.equal(verified.purge_explained_rows, 0);
});

test('absent preload protects actual version-zero terminal and rejects its disappearance or replacement', async () => {
  const s = new Storage(), baseline = await s.observe();
  s.seed(add('created.parquet', 9));
  const terminal = await s.read();
  assert.equal((await result(s, baseline, terminal)).status, 'preserved');
  s.objects.clear();
  const disappeared = await result(s, baseline, terminal);
  assert.equal(disappeared.status, 'failed');
  assert.equal(disappeared.current?.observation, 'observed_absent');
  assert.equal(disappeared.disposition, null);
  s.put(0, { protocol: legacyProtocol }, { metaData: meta('replacement') }, info('CREATE TABLE'), add('replacement.parquet', 9));
  assert.equal((await result(s, baseline, terminal)).status, 'failed');
});

test('an actual baseline cannot become an absent terminal; a table appearing after absent terminal is not absence proof', async () => {
  const s = new Storage(), absent = await s.observe();
  s.seed(add('created.parquet'));
  const actual = await s.read();
  assert.equal((await result(s, actual, absent)).status, 'failed');
  const appeared = await result(s, absent, absent);
  assert.equal(appeared.status, 'unknown');
  assert.equal(appeared.current?.observation, 'table');
  assert.equal(appeared.disposition, null);
});

test('absent baseline requires all JSON history from creation even when current checkpoint is replayable', async () => {
  const s = new Storage(), baseline = await s.observe();
  s.seed(add('created.parquet')).put(1, info('INSERT'), add('during-cell.parquet'));
  const terminal = await s.read();
  s.objects.clear();
  s.objects.set(prefix + '00000000000000000002.checkpoint.parquet',
    checkpoint([{ protocol: legacyProtocol }, { metaData: meta() }, add('created.parquet'), add('during-cell.parquet')]));
  const current = await s.read();
  assert.equal(current.row_count, 20);
  const verified = await result(s, baseline, terminal);
  assert.equal(verified.status, 'unknown');
  assert.match(verified.reason!, /history/);
});

test('an absent baseline does not waive unexplained removal, missing stats or unknown creation operation', async () => {
  const s = new Storage(), baseline = await s.observe();
  s.seed(add('created.parquet')).put(1, info('INSERT'), add('during-cell.parquet', 20));
  const terminal = await s.read();
  s.put(2, info('DELETE', { engineInfo: 'other' }), remove('during-cell.parquet'));
  assert.equal((await result(s, baseline, terminal)).status, 'failed');
  s.objects.clear();
  s.seed(add('missing-stats.parquet', null));
  const missingStats = await s.read();
  assert.equal((await result(s, baseline, missingStats)).status, 'unknown');
  s.put(0, { protocol: legacyProtocol }, { metaData: meta() }, info('RESTORE'), add('restored.parquet'));
  const unknownCreation = await s.read();
  assert.equal((await result(s, baseline, unknownCreation)).status, 'unknown');
});
