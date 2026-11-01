import { test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { FakeAzure } from './test_support.js';
import { Failure, canonical_json, uuid, publisher_registry } from './bootstrap.js';
import { PublisherControl, READY, FENCED, CLEANING, UnknownTransition, canonical_uuid, encode_registry } from './publisher_control.js';

let azure: FakeAzure, control: PublisherControl, generation: string, publisher: string;
beforeEach(() => {
  azure = new FakeAzure(); control = new PublisherControl(azure);
  generation = azure.metadata.resetgeneration; publisher = uuid();
});
const rejects = (action: () => Promise<unknown>) => assert.rejects(action, Failure);
test('normal admission and definitive drain preserve unrelated metadata', async () => {
  await control.admit(publisher, generation);
  assert.equal(azure.registry()[publisher].state, 'active');
  await control.clean_release(publisher, generation, true);
  assert.deepEqual(azure.registry(), {}); assert.equal(azure.metadata.custom, 'preserved');
});
test('fence preserves generation/members and permits only existing clean drain', async () => {
  await control.admit(publisher, generation); await control.fence(generation);
  assert.equal(azure.metadata.resetgeneration, generation);
  await rejects(() => control.admit(uuid(), generation)); await rejects(() => control.drained(generation));
  await control.clean_release(publisher, generation, true); await control.drained(generation);
});
test('uncertain member never expires or releases after logical reconciliation', async () => {
  await control.admit(publisher, generation); await control.uncertain(publisher, generation); await control.fence(generation);
  for (let i = 0; i < 3; i++) {
    await rejects(() => control.clean_release(publisher, generation, true));
    await rejects(() => control.claim_cleanup(uuid(), generation));
    await rejects(() => control.drained(generation));
  }
  assert.equal(azure.registry()[publisher].state, 'uncertain');
});
test('active crashed or suspended registration never expires', async () => {
  await control.admit(publisher, generation); await control.fence(generation);
  for (let i = 0; i < 3; i++) await rejects(() => control.drained(generation));
  assert.ok(publisher in azure.registry());
});
test('child drain then fresh parent completion is one CAS and child is never adopted', async () => {
  await control.fence(generation);
  const cleaner = uuid(), next = uuid(), child = uuid(), parent = uuid();
  await control.claim_cleanup(cleaner, generation); await control.finish_cleanup(cleaner, generation, next, true);
  await control.admit(child, next, true); await rejects(() => control.admit(parent, next, true));
  await control.clean_release(child, next, true); await control.admit(parent, next, true);
  await rejects(() => control.claim_cleanup(cleaner, next));
  await control.bootstrap_owner(parent, next);
  await rejects(() => control.complete_bootstrap(child, next, true));
  const before = azure.events.length;
  await control.complete_bootstrap(parent, next, true);
  assert.equal(azure.events.length, before + 1);
  assert.equal(azure.metadata.resetstate, READY); assert.deepEqual(azure.registry(), {});
});
for (const winner of ['parent', 'cleanup']) test(`${winner} wins gap after child drain and excludes competitor`, async () => {
  await control.fence(generation);
  const child = uuid(), parent = uuid(), cleaner = uuid();
  await control.admit(child, generation, true); await control.clean_release(child, generation, true);
  if (winner === 'parent') {
    await control.admit(parent, generation, true);
    await rejects(() => control.claim_cleanup(cleaner, generation)); await control.bootstrap_owner(parent, generation);
  } else {
    await control.claim_cleanup(cleaner, generation); await rejects(() => control.admit(parent, generation, true));
    await control.finish_cleanup(cleaner, generation, uuid(), true);
    await rejects(() => control.admit(parent, generation, true)); assert.deepEqual(azure.registry(), {});
  }
});
for (const role of ['child', 'parent']) test(`unknown ${role} outcome blocks parent completion and cleanup`, async () => {
  await control.fence(generation); await control.admit(publisher, generation, true); await control.uncertain(publisher, generation);
  await rejects(() => control.admit(uuid(), generation, true));
  for (const drained of [true, false]) await rejects(() => control.complete_bootstrap(publisher, generation, drained));
  await rejects(() => control.claim_cleanup(uuid(), generation)); assert.equal(azure.metadata.resetstate, FENCED);
});
test('unknown CAS is single-shot even when applied; no repeat identity adoption', async () => {
  const original = azure.set_control.bind(azure); let calls = 0;
  azure.set_control = async (metadata, etag) => { calls++; await original(metadata, etag); throw new Failure('response lost'); };
  await assert.rejects(() => control.admit(publisher, generation), UnknownTransition);
  assert.equal(calls, 1); assert.ok(publisher in azure.registry());
  azure.set_control = original; await rejects(() => control.admit(publisher, generation));
  await control.fence(generation); await rejects(() => control.drained(generation));
});
test('admission/fence ETag race fails closed without adding a member', async () => {
  const original = azure.set_control.bind(azure);
  azure.set_control = async (metadata, etag) => {
    azure.metadata.resetstate = FENCED; azure.etag = uuid(); await original(metadata, etag);
  };
  await assert.rejects(() => control.admit(publisher, generation), UnknownTransition);
  assert.deepEqual(azure.registry(), {}); assert.equal(azure.metadata.resetstate, FENCED);
});
test('seventeenth publisher and total header budget fail closed', async () => {
  for (let i = 0; i < 16; i++) await control.admit(uuid(), generation);
  await rejects(() => control.admit(uuid(), generation)); assert.equal(Object.keys(azure.registry()).length, 16);
  azure = new FakeAzure(); control = new PublisherControl(azure); azure.metadata.custom = 'x'.repeat(8000);
  await rejects(() => control.admit(publisher, azure.metadata.resetgeneration)); assert.deepEqual(azure.registry(), {});
});
for (const [field, value] of [
  ['resetstate', 'ready'], ['resetstate', 'fenced'], ['resetstate', 'alien'],
  ['publisherregistryversion', undefined], ['publisherregistryversion', '2'], ['occupancy', undefined],
  ['occupancy', 'stopped'], ['occupancy', 'testing'], ['resetgeneration', 'invalid'], ['publishers', '{"bad":{}}'],
  ['sandboxid', 'foreign'],
] as const) test(`authority rejects invalid ${field}=${value}`, async () => {
  if (value === undefined) delete azure.metadata[field]; else azure.metadata[field] = value;
  await rejects(() => control.read()); assert.deepEqual(azure.events, []);
});
test('registry foreign generation fails even with canonical encoding', async () => {
  azure.metadata.publishers = canonical_json({ [publisher]: { generation: uuid(), state: 'active' } });
  await rejects(() => control.read());
});
test('test occupancy blocks fencing, bootstrap, exclusive custody and cleanup', async () => {
  azure.metadata.occupancy = 'testing'; azure.metadata.occupancyid = uuid();
  await rejects(() => control.fence(generation)); await rejects(() => control.claim_exclusive(uuid(), generation));
  azure.metadata.resetstate = FENCED;
  await rejects(() => control.admit(publisher, generation, true)); await rejects(() => control.claim_cleanup(uuid(), generation));
});
test('generation rotation requires exact held active cleanup and fresh UUID', async () => {
  const cleaner = uuid();
  await rejects(() => control.claim_cleanup(cleaner, generation));
  await control.fence(generation); await rejects(() => control.claim_cleanup(cleaner, uuid()));
  await control.claim_cleanup(cleaner, generation);
  await rejects(() => control.finish_cleanup(cleaner, generation, generation, true));
  await rejects(() => control.finish_cleanup(cleaner, generation, uuid(), false));
  await rejects(() => control.finish_cleanup(uuid(), generation, uuid(), true));
  assert.equal(azure.metadata.resetstate, CLEANING);
});
test('read-only drain snapshot grants no custody against later bootstrap', async () => {
  await control.fence(generation); await control.drained(generation); await control.admit(publisher, generation, true);
  await rejects(() => control.claim_cleanup(uuid(), generation)); await rejects(() => control.cleanup_operator(uuid(), generation));
  assert.deepEqual(Object.keys(azure.registry()), [publisher]);
});
test('cleanup custody rejects every competing transition throughout deletion', async () => {
  await control.fence(generation); await control.claim_cleanup(publisher, generation);
  for (let step = 0; step < 3; step++) {
    await control.cleanup_operator(publisher, generation);
    for (const action of [
      () => control.admit(uuid(), generation, true), () => control.admit(uuid(), generation),
      () => control.claim_cleanup(uuid(), generation), () => control.fence(generation),
      () => control.complete_bootstrap(publisher, generation, true), () => control.clean_release(publisher, generation, true),
    ]) await rejects(action);
  }
  assert.equal(azure.metadata.resetstate, CLEANING); assert.deepEqual(Object.keys(azure.registry()), [publisher]);
});
for (const winner of ['bootstrap', 'cleanup']) test(`concurrent ${winner} CAS excludes stale competitor`, async () => {
  await control.fence(generation);
  const original = azure.set_control.bind(azure), competitor = new PublisherControl(azure), cleaner = uuid();
  azure.set_control = async (metadata, etag) => {
    azure.set_control = original;
    if (winner === 'bootstrap') await competitor.admit(publisher, generation, true);
    else await competitor.claim_cleanup(cleaner, generation);
    await original(metadata, etag);
  };
  await assert.rejects(() => winner === 'bootstrap' ? control.claim_cleanup(cleaner, generation) :
    control.admit(publisher, generation, true), UnknownTransition);
  assert.deepEqual(Object.keys(azure.registry()), [winner === 'bootstrap' ? publisher : cleaner]);
  assert.equal(azure.metadata.resetstate, winner === 'bootstrap' ? FENCED : CLEANING);
});
test('cleanup completion enables only fresh generation and old operator cannot act', async () => {
  await control.fence(generation); await control.claim_cleanup(publisher, generation);
  await rejects(() => control.admit(uuid(), generation, true));
  const next = uuid(); await control.finish_cleanup(publisher, generation, next, true);
  await rejects(() => control.admit(uuid(), generation, true));
  const child = uuid(); await control.admit(child, next, true);
  await rejects(() => control.claim_cleanup(uuid(), next)); await rejects(() => control.cleanup_operator(publisher, next));
});
for (const phase of ['claim', 'finish', 'parent completion']) test(`unknown ${phase} CAS is never reported as success`, async () => {
  await control.fence(generation);
  if (phase === 'finish') await control.claim_cleanup(publisher, generation);
  if (phase === 'parent completion') await control.admit(publisher, generation, true);
  const original = azure.set_control.bind(azure);
  azure.set_control = async (metadata, etag) => {
    if (phase === 'claim') await original(metadata, etag);
    throw new Failure('timeout');
  };
  await assert.rejects(() => phase === 'claim' ? control.claim_cleanup(publisher, generation) :
    phase === 'finish' ? control.finish_cleanup(publisher, generation, uuid(), true) :
      control.complete_bootstrap(publisher, generation, true), UnknownTransition);
  assert.equal(azure.metadata.resetstate, phase === 'parent completion' ? FENCED : CLEANING);
  await rejects(() => control.admit(uuid(), generation, true));
});
test('uncertain cleanup operator cannot finish or abort', async () => {
  await control.fence(generation); await control.claim_cleanup(publisher, generation); await control.uncertain(publisher, generation);
  await rejects(() => control.finish_cleanup(publisher, generation, uuid(), true));
  await rejects(() => control.abort_cleanup(publisher, generation, true));
  assert.equal(azure.registry()[publisher].state, 'uncertain');
});
test('abort requires exact custody and no deletion, preserves captured generation', async () => {
  await control.fence(generation); await rejects(() => control.abort_cleanup(publisher, generation, true));
  await control.claim_cleanup(publisher, generation);
  await rejects(() => control.abort_cleanup(publisher, generation, false));
  await rejects(() => control.abort_cleanup(uuid(), generation, true));
  await rejects(() => control.abort_cleanup(publisher, uuid(), true));
  await control.abort_cleanup(publisher, generation, true);
  assert.equal(azure.metadata.resetstate, FENCED); assert.equal(azure.metadata.resetgeneration, generation);
  assert.deepEqual(azure.registry(), {});
});
test('canonical wire is byte-identical to Rust with sorted UUIDs and entry keys', () => {
  const first = '00000000-0000-0000-0000-000000000001', last = 'ffffffff-ffff-ffff-ffff-ffffffffffff';
  const value = encode_registry(azure.metadata, { [last]: { state: 'uncertain', generation }, [first]: { state: 'active', generation } });
  assert.equal(value.publishers, `{"${first}":{"generation":"${generation}","state":"active"},"${last}":{"generation":"${generation}","state":"uncertain"}}`);
  assert.deepEqual(publisher_registry(value), JSON.parse(value.publishers));
});
for (const invalid of ['', '{}', 'ABCDEFAB-0000-0000-0000-000000000000', '{00000000-0000-0000-0000-000000000001}', null, 1])
  test(`canonical UUID rejects ${JSON.stringify(invalid)}`, () => assert.throws(() => canonical_uuid(invalid), Failure));
test('released occupancy ignores stale owner just like Rust', async () => {
  azure.metadata.occupancyid = 'stale non-UUID'; await control.read();
});
test('occupant owner and generation predicates checked in admission CAS', async () => {
  azure.metadata.occupancy = 'testing'; azure.metadata.occupancyid = uuid();
  await rejects(() => control.admit(publisher, generation, false, uuid()));
  await control.admit(publisher, generation, false, azure.metadata.occupancyid);
  assert.ok(publisher in azure.registry());
});
test('exclusive operator blocks admission, occupy-compatible state, cleanup and stop until completion', async () => {
  await control.claim_exclusive(publisher, generation);
  assert.equal(azure.metadata.resetstate, FENCED);
  await rejects(() => control.claim_exclusive(uuid(), generation));
  await rejects(() => control.admit(uuid(), generation)); await rejects(() => control.admit(uuid(), generation, true));
  await rejects(() => control.claim_cleanup(uuid(), generation));
  await control.complete_bootstrap(publisher, generation, true); assert.equal(azure.metadata.resetstate, READY);
});
