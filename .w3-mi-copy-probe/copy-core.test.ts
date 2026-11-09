import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  NATIVE, SEMANTICS, checkAccess, checkFederation, copySnapshot, destination, manifest, proveRows,
  readSnapshot, relativeName, replay, sha256, type Manifest, type Snapshot, type Transport,
} from './copy-core.js';

const bound = { maxFiles: 20, maxBytes: 1024 * 1024, maxRows: 100 };
const run = '11111111-1111-4111-8111-111111111111';
const owner = '22222222-2222-4222-8222-222222222222';
const target = destination({ owner, prefix: 'qualification/' + owner, container: 'test-data' });
const zero = '_delta_log/00000000000000000000.json';
const one = '_delta_log/00000000000000000001.json';
const schema = JSON.stringify({ type: 'struct', fields: [{ name: 'value', type: 'long', nullable: true, metadata: {} }] });
const fixtureRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures', 'r6');
const r6ProtocolBytes = fs.readFileSync(path.join(fixtureRoot, 'protocol.json'));
const r6MetadataBytes = fs.readFileSync(path.join(fixtureRoot, 'metadata.json'));
const actions = [
  { protocol: { minReaderVersion: 1, minWriterVersion: 2 } },
  { metaData: { id: run, format: { provider: 'parquet', options: {} }, configuration: {},
    partitionColumns: [], schemaString: schema } },
  { add: { path: 'a.parquet', size: 3, partitionValues: {} } },
];
function fixture(logActions: unknown[] = actions): Snapshot {
  // Deliberately fake payload bytes: control-flow tests cannot become a qualification dataset.
  const bytes = new Map([[zero, Buffer.from(logActions.map(action => JSON.stringify(action)).join('\n') + '\n')],
    ['a.parquet', Buffer.from('one')]]);
  const m = manifest({ schema_version: 1, run_id: run, table: 'synthetic_only', latestVersion: 0, rowCount: 1,
    schemaString: schema,
    files: [...bytes].map(([name, content]) => ({ path: name, size: content.length, sha256: sha256(content) })),
    writers: [{ id: 'synthetic-writer', service: 'synthetic-only', was_running: true, exit_code: 143,
      finished_at: '2026-01-01T00:00:00.123456789Z', oom_killed: false }], semantics: SEMANTICS }, bound);
  return { manifest: m, manifestSha256: sha256(JSON.stringify(m)), bytes, ...replay(m, bytes) };
}
function producerFixture(protocolBytes = r6ProtocolBytes, metadataBytes = r6MetadataBytes): Snapshot {
  const metadataAction = JSON.parse(metadataBytes.toString('utf8')).metaData;
  const addLine = Buffer.from(JSON.stringify({ add: { path: 'a.parquet', size: 3, partitionValues: {} } }) + '\n');
  // Only protocol and metadata are producer bytes; the Add action and payload remain synthetic.
  const log = Buffer.concat([protocolBytes, Buffer.from('\n'), metadataBytes, Buffer.from('\n'), addLine]);
  const bytes = new Map([[zero, log], ['a.parquet', Buffer.from('one')]]);
  const m = manifest({ schema_version: 1, run_id: run, table: 'sqlserver_active_sessions', latestVersion: 0,
    rowCount: 1, schemaString: metadataAction.schemaString,
    files: [...bytes].map(([name, content]) => ({ path: name, size: content.length, sha256: sha256(content) })),
    writers: [{ id: 'synthetic-writer', service: 'synthetic-only', was_running: true, exit_code: 143,
      finished_at: '2026-01-01T00:00:00.123456789Z', oom_killed: false }], semantics: SEMANTICS }, bound);
  return { manifest: m, manifestSha256: sha256(JSON.stringify(m)), bytes, ...replay(m, bytes) };
}
const decode = (bytes: Buffer) => ({ rows: bytes.toString() === 'one' ? 1 : 0, fields: ['value'] });
function fakeTransport() {
  const stored = new Map<string, { bytes: Buffer; etag: string }>();
  const puts: string[] = [], gets: string[] = [];
  const transport: Transport = {
    async list() { return [...stored].map(([name, value]) => ({ name, etag: value.etag })); },
    async put(name, bytes, _requestId, capture) {
      puts.push(name);
      assert.equal(stored.has(name), false);
      const etag = `"${puts.length}"`;
      stored.set(name, { bytes: Buffer.from(bytes), etag });
      capture({ status: 201, bodyBase64: '', headers: { etag } });
      return etag;
    },
    async get(name, etag, capture) {
      gets.push(name);
      const value = stored.get(name)!;
      assert.equal(value.etag, etag);
      capture({ status: 200, bodyBase64: value.bytes.toString('base64'), headers: { etag } });
      return Buffer.from(value.bytes);
    },
  };
  return { transport, stored, puts, gets };
}
test('relative names reject traversal, URI, encoding, aliases and Windows paths', () => {
  for (const value of ['../x', 'a/../x', '/x', 'abfss://host/x', 'a%2fb.parquet', 'a\\b', './x', 'x/', 'x//y', 'x.'])
    assert.throws(() => relativeName(value));
  assert.equal(relativeName('parts/a-b_1.parquet'), 'parts/a-b_1.parquet');
});
test('manifest rejects duplicate paths, missing stopped-writer proof and false nonempty count', () => {
  const m = fixture().manifest;
  assert.throws(() => manifest({ ...m, files: [...m.files, m.files[0]] }, bound));
  assert.throws(() => manifest({ ...m, writers: [] }, bound));
  assert.throws(() => manifest({ ...m, rowCount: 0 }, bound));
  assert.throws(() => manifest({ ...m, semantics: 'all flushed' }, bound));
  for (const mutation of [{ exit_code: 137 }, { oom_killed: true }, { was_running: false }, { finished_at: '0001-01-01T00:00:00Z' }])
    assert.throws(() => manifest({ ...m, writers: [{ ...m.writers[0], ...mutation }] }, bound));
});
test('accepts exact R6 DFE protocol and catalog metadata action bytes', () => {
  const snapshot = producerFixture();
  assert.equal(snapshot.tableId, '1325c57f-dc96-4680-9c30-f2c876ec3f7d');
  assert.equal(snapshot.schemaFields.length, 10);
  assert.deepEqual(snapshot.active, ['a.parquet']);
});
test('rejects unapproved protocol features, protocol changes and catalog schema evolution', () => {
  const protocol = JSON.parse(r6ProtocolBytes.toString('utf8'));
  for (const mutation of [
    { ...protocol, protocol: { ...protocol.protocol, readerFeatures: ['deletionVectors'] } },
    { ...protocol, protocol: { ...protocol.protocol, writerFeatures: ['invariants', 'unknown'] } },
    { protocol: { minReaderVersion: 3, minWriterVersion: 8, readerFeatures: [], writerFeatures: ['invariants'] } },
  ]) assert.throws(() => producerFixture(Buffer.from(JSON.stringify(mutation)), r6MetadataBytes));
  const metadata = JSON.parse(r6MetadataBytes.toString('utf8'));
  const history = JSON.parse(metadata.metaData.configuration['catalog.schemaHistory']);
  const evolved = structuredClone(metadata);
  evolved.metaData.configuration['catalog.schemaHistory'] = JSON.stringify([...history, { ...history[0], version: 2 }]);
  assert.throws(() => producerFixture(r6ProtocolBytes, Buffer.from(JSON.stringify(evolved))));
  const unknownConfig = structuredClone(metadata);
  unknownConfig.metaData.configuration['catalog.unknown'] = 'true';
  assert.throws(() => producerFixture(r6ProtocolBytes, Buffer.from(JSON.stringify(unknownConfig))));
  const missingHistory = structuredClone(metadata);
  delete missingHistory.metaData.configuration['catalog.schemaHistory'];
  assert.throws(() => producerFixture(r6ProtocolBytes, Buffer.from(JSON.stringify(missingHistory))));

  const base = producerFixture();
  const changedProtocol = Buffer.from(JSON.stringify({ protocol: {
    minReaderVersion: 1, minWriterVersion: 2,
  } }) + '\n' + JSON.stringify({ txn: { appId: 'synthetic', version: 1 } }) + '\n');
  const bytes = new Map(base.bytes).set(one, changedProtocol);
  const m = manifest({ ...base.manifest, latestVersion: 1,
    files: [...bytes].map(([name, content]) => ({ path: name, size: content.length, sha256: sha256(content) })) }, bound);
  assert.throws(() => replay(m, bytes), /protocol changes/);
});
test('replay rejects missing version, historical payload, checkpoint, DV and unsupported protocol', () => {
  const base = fixture();
  assert.throws(() => replay({ ...base.manifest, latestVersion: 1 }, base.bytes));
  assert.throws(() => replay({ ...base.manifest, schemaString: '{}' }, base.bytes));
  assert.throws(() => replay({ ...base.manifest, files: base.manifest.files.filter(file => file.path !== 'a.parquet') }, base.bytes));
  assert.throws(() => replay({ ...base.manifest,
    files: [...base.manifest.files, { path: '_delta_log/_last_checkpoint', size: 1, sha256: '0'.repeat(64) }] }, base.bytes));
  assert.throws(() => fixture([...actions.slice(0, 2), { add: {
    path: 'a.parquet', size: 3, partitionValues: {}, deletionVector: { storageType: 'u' },
  } }]));
  assert.throws(() => fixture([{ protocol: { minReaderVersion: 3, minWriterVersion: 7 } }, ...actions.slice(1)]));
});
test('preserves only the exact empty writer-lock sentinel without treating it as payload', () => {
  const base = fixture(), lock = '.arrow-lake-writer.lock', empty = Buffer.alloc(0);
  const bytes = new Map(base.bytes).set(lock, empty);
  const m = manifest({ ...base.manifest, files: [...bytes].map(([name, content]) =>
    ({ path: name, size: content.length, sha256: sha256(content) })) }, bound);
  assert.deepEqual(replay(m, bytes).active, ['a.parquet']);
  assert.throws(() => manifest({ ...m, files: m.files.map(file =>
    file.path === lock ? { ...file, sha256: '0'.repeat(64) } : file) }, bound));
  assert.throws(() => manifest({ ...m, files: m.files.map(file =>
    file.path === lock ? { ...file, size: 1 } : file) }, bound));
  assert.throws(() => manifest({ ...m, files: [...m.files, {
    path: 'unrelated.lock', size: 0, sha256: sha256(empty),
  }] }, bound));
  assert.throws(() => manifest({ ...m, writers: [] }, bound));
});
test('initial commit protocol and metadata resolution does not depend on physical action order', () => {
  const snapshot = fixture([...actions].reverse());
  assert.deepEqual(snapshot.active, ['a.parquet']);
  assert.equal(snapshot.tableId, run);
});
test('full history keeps removed payload before logs and counts only latest active Adds', () => {
  const base = fixture();
  base.bytes.set(one, Buffer.from(JSON.stringify({ remove: { path: 'a.parquet' } }) + '\n' +
    JSON.stringify({ add: { path: 'b.parquet', size: 3, partitionValues: {} } }) + '\n'));
  base.bytes.set('b.parquet', Buffer.from('one'));
  const m: Manifest = { ...base.manifest, latestVersion: 1,
    files: [...base.bytes].map(([name, bytes]) => ({ path: name, size: bytes.length, sha256: sha256(bytes) })) };
  const snapshot = { ...base, manifest: m, ...replay(m, base.bytes) };
  assert.deepEqual(snapshot.payloads, ['a.parquet', 'b.parquet']);
  assert.deepEqual(snapshot.active, ['b.parquet']);
  assert.equal(proveRows(snapshot, decode, bound), 1);
});
test('actual decoded count and column names, not declared log stats, gate nonempty proof', () => {
  const snapshot = fixture();
  assert.equal(proveRows(snapshot, decode, bound), 1);
  assert.throws(() => proveRows(snapshot, () => ({ rows: 0, fields: ['value'] }), bound));
  assert.throws(() => proveRows(snapshot, () => ({ rows: 2, fields: ['value'] }), bound));
  assert.throws(() => proveRows(snapshot, () => ({ rows: 1, fields: ['other'] }), bound));
});
const jwt = (claims: unknown) => 'header.' + Buffer.from(JSON.stringify(claims)).toString('base64url') + '.signature';
test('all federation inputs and exact issuer/subject/audience required before exchange', () => {
  const env = { AZURE_CLIENT_ID: NATIVE.client, AZURE_TENANT_ID: NATIVE.tenant, AZURE_FEDERATED_TOKEN_FILE: '/token' };
  const claims = { iss: NATIVE.issuer, sub: NATIVE.subject, aud: [NATIVE.audience], exp: 200 };
  checkFederation(env, jwt(claims), '/token', 100);
  assert.throws(() => checkFederation({}, jwt(claims), '/token', 100));
  for (const key of Object.keys(env))
    assert.throws(() => checkFederation({ ...env, [key]: '' }, jwt(claims), '/token', 100));
  for (const mutation of [{ iss: 'https://other/' }, { sub: 'other' }, { aud: ['other'] }, { exp: 120 }])
    assert.throws(() => checkFederation(env, jwt({ ...claims, ...mutation }), '/token', 100));
});
test('storage access token must describe native tenant, object, client and audience', () => {
  const claims = { tid: NATIVE.tenant, oid: NATIVE.object, appid: NATIVE.client,
    aud: 'https://storage.azure.com/', iss: `https://sts.windows.net/${NATIVE.tenant}/`, exp: 200 };
  checkAccess(jwt(claims), 100);
  for (const mutation of [{ oid: NATIVE.client }, { tid: run }, { appid: owner }, { aud: 'https://management.azure.com/' }])
    assert.throws(() => checkAccess(jwt({ ...claims, ...mutation }), 100));
});
test('create sequence is intent, every payload, unchanged logs; all bytes read twice; never publishes', async () => {
  const fake = fakeTransport(), evidence: Record<string, unknown>[] = [];
  let rechecks = 0;
  await copySnapshot(fixture(), target, fake.transport, decode, bound, value => evidence.push(value), () => { rechecks++; });
  assert.deepEqual(fake.puts, [target.prefix + '/_copy-owner.json', target.prefix + '/table/a.parquet', target.prefix + '/table/' + zero]);
  assert.equal(fake.gets.length, 6);
  assert.equal(rechecks, 3);
  const proof = evidence.at(-1)!;
  assert.equal(proof.event, 'copy-verified-not-published');
  assert.equal(proof.readyMarkerCreated, false);
  assert.equal(proof.adxAttempted, false);
  assert.equal(proof.qapAttempted, false);
  assert.deepEqual(fake.stored.get(target.prefix + '/table/' + zero)?.bytes, fixture().bytes.get(zero));
});
test('copies the exact empty lock sentinel with the data, before unchanged logs', async () => {
  const base = fixture(), lock = '.arrow-lake-writer.lock';
  base.bytes.set(lock, Buffer.alloc(0));
  const m = manifest({ ...base.manifest, files: [...base.bytes].map(([name, content]) =>
    ({ path: name, size: content.length, sha256: sha256(content) })) }, bound);
  const snapshot = { ...base, manifest: m, ...replay(m, base.bytes) };
  const fake = fakeTransport();
  await copySnapshot(snapshot, target, fake.transport, decode, bound, () => {}, () => {});
  assert.deepEqual(fake.puts, [target.prefix + '/_copy-owner.json', target.prefix + '/table/a.parquet',
    target.prefix + '/table/' + lock, target.prefix + '/table/' + zero]);
  assert.deepEqual(fake.stored.get(target.prefix + '/table/' + lock)?.bytes, Buffer.alloc(0));
});
test('preexisting destination fails before any PUT; no adoption', async () => {
  const fake = fakeTransport();
  fake.stored.set(target.prefix + '/old', { bytes: Buffer.from('x'), etag: '"old"' });
  await assert.rejects(copySnapshot(fixture(), target, fake.transport, decode, bound, () => {}, () => {}), /collision/);
  assert.equal(fake.puts.length, 0);
});
test('unknown admitted PUT is never replayed and logs remain unsubmitted', async () => {
  const fake = fakeTransport();
  const original = fake.transport.put;
  fake.transport.put = async (...args) => { await original(...args); throw new Error('simulated lost response'); };
  const evidence: Record<string, unknown>[] = [];
  await assert.rejects(copySnapshot(fixture(), target, fake.transport, decode, bound, value => evidence.push(value), () => {}));
  assert.equal(fake.puts.length, 1);
  assert.equal(fake.stored.size, 1);
  assert.equal(evidence.some(value => value.event === 'copy-verified-not-published'), false);
});
test('missing success receipt or changed readback stops without publication', async () => {
  const missing = fakeTransport();
  missing.transport.put = async () => '"unproven"';
  await assert.rejects(copySnapshot(fixture(), target, missing.transport, decode, bound, () => {}, () => {}), /201/);
  const corrupt = fakeTransport(), original = corrupt.transport.get;
  corrupt.transport.get = async (...args) => { await original(...args); return Buffer.from('bad'); };
  await assert.rejects(copySnapshot(fixture(), target, corrupt.transport, decode, bound, () => {}, () => {}), /hash/);
  assert.equal(corrupt.puts.length, 1);
});
test('late extra inventory or source change blocks final proof without cleanup', async () => {
  const fake = fakeTransport(), original = fake.transport.list;
  let lists = 0;
  fake.transport.list = async () => ++lists === 1 ? original() :
    [...await original(), { name: target.prefix + '/foreign', etag: '"foreign"' }];
  const evidence: Record<string, unknown>[] = [];
  await assert.rejects(copySnapshot(fixture(), target, fake.transport, decode, bound, value => evidence.push(value), () => {}), /inventory/);
  assert.equal(fake.stored.size, 3);
  assert.equal(evidence.some(value => value.event === 'copy-verified-not-published'), false);
  let rechecks = 0;
  await assert.rejects(copySnapshot(fixture(), target, fakeTransport().transport, decode, bound, () => {}, () => {
    if (++rechecks === 2) throw new Error('source changed');
  }));
});
test('real filesystem snapshot rejects extra files, changed bytes and symlink/hardlink aliases', { skip: process.platform !== 'linux' }, t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'w3-copy-unit-'));
  t.after(() => fs.rmSync(root, { recursive: true }));
  const table = path.join(root, 'table');
  fs.mkdirSync(path.join(table, '_delta_log'), { recursive: true });
  const sample = fixture(), manifestFile = path.join(root, 'manifest.json');
  sample.bytes.set('.arrow-lake-writer.lock', Buffer.alloc(0));
  const sampleManifest = { ...sample.manifest, files: [...sample.bytes].map(([name, content]) =>
    ({ path: name, size: content.length, sha256: sha256(content) })) };
  for (const [name, content] of sample.bytes) fs.writeFileSync(path.join(table, ...name.split('/')), content);
  const raw = Buffer.from(JSON.stringify(sampleManifest));
  fs.writeFileSync(manifestFile, raw);
  const read = () => readSnapshot(manifestFile, sha256(raw), table, bound, run, 'synthetic_only');
  assert.equal(read().manifest.rowCount, 1);
  fs.writeFileSync(path.join(table, 'unrelated'), 'x');
  assert.throws(read);
  fs.unlinkSync(path.join(table, 'unrelated'));
  fs.writeFileSync(path.join(table, 'a.parquet'), 'two');
  assert.throws(read);
  fs.writeFileSync(path.join(table, 'a.parquet'), 'one');
  fs.linkSync(path.join(table, 'a.parquet'), path.join(root, 'hardlink'));
  assert.throws(read);
  fs.unlinkSync(path.join(root, 'hardlink'));
  fs.unlinkSync(path.join(table, 'a.parquet'));
  fs.symlinkSync(manifestFile, path.join(table, 'a.parquet'));
  assert.throws(read);
});
