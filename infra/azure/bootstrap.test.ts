import { test, beforeEach, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import * as path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import * as b from './bootstrap.js';
import { FakeAzure, SUB } from './test_support.js';
import { PublisherControl, READY, FENCED, CLEANING, UnknownTransition } from './publisher_control.js';

let azure: FakeAzure, directory: string, recordPath: string, manifest: string, output: string, generation: string;
let elapsed = 0;
const realRun = b.run_tree;
const parentEnvironment = process.env;
const receipt = (generation: string) => JSON.stringify({ ...b.RECEIPT_FIELDS, resetGeneration: generation }) + '\n';
const failure = (action: () => Promise<unknown>) => assert.rejects(action, b.Failure);
beforeEach(() => {
  // Nx loads private .env files into its child environment; unit tests must never adopt them.
  process.env = Object.fromEntries(Object.entries(parentEnvironment)
    .filter(([key]) => !/^(AZURE_|ARROWCOL_)/.test(key)));
  azure = new FakeAzure(); generation = azure.metadata.resetgeneration;
  directory = path.join(b.ROOT, 'infra/azure/.azure-test-' + b.uuid()); fs.mkdirSync(directory);
  recordPath = path.join(directory, '.env.azure.reset.json');
  manifest = path.join(directory, 'tables.json'); output = path.join(directory, '.env.azure');
  fs.writeFileSync(manifest, '["Logs"]');
  elapsed = 0;
  mock.method(b.runtime, 'now', () => elapsed);
  mock.method(b.runtime, 'sleep', async (seconds: number) => { elapsed += seconds; });
  mock.method(b.runtime, 'run_tree', async () => ({ returncode: 0, stdout: receipt(generation), stderr: '' }));
});
afterEach(() => {
  mock.restoreAll(); process.env = parentEnvironment;
  fs.rmSync(directory, { recursive: true, force: true });
});
const execute = () => b._unqualified_reset_sequence(azure, ['stop'], recordPath);
const resume = () => b._unqualified_reset_resume(azure, recordPath);
const makeRecord = (): b.ResetRecord => ({ sandbox: azure.c.sandbox, operator: b.uuid(), generation, next: b.uuid() });
const initialize = () => {
  azure.logs.set('Logs/_delta_log/00000000000000000000.json', Buffer.from('{"protocol":{}}\n{"metaData":{}}'));
};
async function custody(record = makeRecord()): Promise<b.ResetRecord> {
  b.write_reset_record(recordPath, record);
  const control = new PublisherControl(azure);
  await control.fence(generation); await control.claim_cleanup(record.operator, generation);
  return record;
}

test('default configuration is deterministic and generated values contain no subscription', () => {
  const c = new b.Config({ AZURE_SUBSCRIPTION_ID: SUB });
  assert.equal(c.group, 'rust-playground'); assert.deepEqual([c.minimum, c.maximum], [2, 4]);
  assert.ok(c.generated().ARROWCOL_AZURE_TABLE_ROOT.startsWith('abfss://'));
  assert.ok(!('AZURE_SUBSCRIPTION_ID' in c.generated()));
  assert.equal(c.account, azure.c.account);
});
for (const values of [
  { AZURE_RESOURCE_GROUP: 'production' }, { AZURE_SUBSCRIPTION_ID: 'default' },
  { AZURE_KUSTO_MAX_INSTANCES: '9' }, { AZURE_KUSTO_MIN_INSTANCES: '5', AZURE_KUSTO_MAX_INSTANCES: '4' },
  { AZURE_KUSTO_QAP_HOT_DAYS: '0' }, { AZURE_KUSTO_QAP_HOT_DAYS: '1.5' }, { AZURE_KUSTO_MAX_INSTANCES: 'NaN' },
  { AZURE_STORAGE_CHECKPOINT_CONTAINER: 'arrowcol' }, { AZURE_OPERATOR_TYPE: 'App' },
  { AZURE_OPERATOR_TYPE: 'Other' }, { AZURE_STORAGE_ACCOUNT: 'Bad_Account' }, { AZURE_KUSTO_DATABASE: 'bad;escape' },
]) test(`configuration rejects ${JSON.stringify(values)}`, () => {
  assert.throws(() => new b.Config({ AZURE_SUBSCRIPTION_ID: SUB, ...values }), b.Failure);
});
test('App operator has distinct explicit application identity', () => {
  assert.equal(new b.Config({ AZURE_SUBSCRIPTION_ID: SUB, AZURE_OPERATOR_TYPE: 'App', AZURE_OPERATOR_APPLICATION_ID: SUB }).operator_application, SUB);
});
test('tenant policy tags supported without overriding exact ownership', () => {
  assert.deepEqual(b.cluster_tags('{"NRMS.KustoPlatform.Classification.1P":"Corp"}'), { 'NRMS.KustoPlatform.Classification.1P': 'Corp' });
  b.owned({ tags: { ...azure.tags, extra: 'allowed' } }, azure.c);
  for (const raw of ['[]', 'null', 'not-json', '{"sandboxId":"x"}', '{"Project":"x"}', '{"k":1}',
    JSON.stringify(Object.fromEntries(Array.from({ length: 16 }, (_, i) => ['tag' + i, 'v'])))])
    assert.throws(() => b.cluster_tags(raw), b.Failure);
  for (const tags of [{ ...azure.tags, sandboxId: 'other' }, { project: 'rust-playground' }, {}])
    assert.throws(() => b.owned({ tags }, azure.c), b.Failure);
  assert.throws(() => b.owned(null, azure.c), b.Failure);
});
test('isolated table root preserves exact account/container and rejects traversal/query/injection', () => {
  const c = azure.c, root = `abfss://${c.data}@${c.account}.dfs.core.windows.net/loadtest/run-example/`;
  const isolated = new b.Config({ AZURE_SUBSCRIPTION_ID: SUB, ARROWCOL_AZURE_TABLE_ROOT: root });
  assert.equal(isolated.table_prefix, 'loadtest/run-example'); assert.equal(isolated.table_root, root);
  for (const unsafe of [root.replace(c.account, 'foreign'), root + '../escape', root + '?token=secret',
    root.replace(c.data + '@', 'foreign@'), root + '%2e%2e', root + '#fragment', root + 'foo;secret'])
    assert.throws(() => new b.Config({ AZURE_SUBSCRIPTION_ID: SUB, ARROWCOL_AZURE_TABLE_ROOT: unsafe }), b.Failure);
});
test('env parser is data-only, preserves expansion literals and rejects invalid syntax', () => {
  const filename = path.join(directory, '.env');
  fs.writeFileSync(filename, "# comment\nexport AZURE_LOCATION='eastus2'\nVALUE=$(echo secret)\n");
  assert.deepEqual(b.load_env(filename), { AZURE_LOCATION: 'eastus2', VALUE: '$(echo secret)' });
  for (const content of ['NOT AN ASSIGNMENT', 'BAD-KEY=1', 'KEY="unclosed', "KEY='"]) {
    fs.writeFileSync(filename, content); assert.throws(() => b.load_env(filename), b.Failure);
  }
  assert.throws(() => b.load_env(filename + '-missing'), b.Failure);
});
test('generated private outputs are atomic 0600 and leave no intermediates', () => {
  b.write_private(output, { EXAMPLE: 'value' });
  assert.equal(fs.statSync(output).mode & 0o777, 0o600); assert.equal(fs.readFileSync(output, 'utf8'), 'EXAMPLE=value\n');
  b.write_private(output, { EXAMPLE: 'updated' }); assert.equal(fs.readFileSync(output, 'utf8'), 'EXAMPLE=updated\n');
  assert.deepEqual(fs.readdirSync(directory).sort(), ['.env.azure', 'tables.json']);
});
test('redacts private names identifiers endpoints principals and bearer credentials with bounded output', () => {
  const c = azure.c;
  const text = c.redact(`${SUB} ${c.account} ${c.cluster} https://secret.example/path user@example.com Bearer secret-token`);
  for (const secret of [SUB, c.account, c.cluster, 'secret.example', 'user@example.com', 'secret-token']) assert.ok(!text.includes(secret));
  assert.equal(c.redact('x'.repeat(3000)).length, 2000);
});
test('preview has no mutating operations and requires no stop command', async () => {
  await b.reset(azure, false, []); assert.deepEqual(azure.events, []);
});
test('execution requires stop command before fence or delete', async () => {
  await assert.rejects(() => b._unqualified_reset_sequence(azure, [], recordPath), /stop-command/);
  assert.deepEqual(azure.events, []);
});
test('execution fences, quiesces then deletes objects/database preserving infrastructure', async () => {
  await execute();
  assert.deepEqual(azure.events[0], ['control', FENCED]); assert.equal(elapsed, 60);
  assert.deepEqual(azure.databases, []); assert.equal(Object.values(azure.items).flat().length, 0);
  assert.equal(azure.metadata.resetstate, FENCED); assert.notEqual(azure.metadata.resetgeneration, generation);
  assert.deepEqual(azure.registry(), {}); assert.equal(fs.existsSync(recordPath), false);
  assert.equal(fs.existsSync(b.database_intent_path(recordPath)), false);
});
test('existing cleanup authority and custody files refuse fresh execution before stop', async () => {
  azure.metadata.resetstate = CLEANING;
  await assert.rejects(execute, /reset --resume/); assert.deepEqual(azure.events, []);
  azure.metadata.resetstate = READY;
  b.write_reset_record(recordPath, makeRecord());
  await assert.rejects(execute, /resume/); assert.deepEqual(azure.events, []);
});
test('recovery preview does not stop or change orphan authority', async () => {
  azure.metadata.occupancy = 'testing'; azure.metadata.occupancyid = b.uuid(); azure.register(undefined, 'uncertain');
  const before = { ...azure.metadata };
  await b.recover(azure, false, ['stop']); assert.deepEqual(azure.metadata, before); assert.deepEqual(azure.events, []);
});
test('recovery requires bound stop receipt then quiesces and rotates orphan authority', async () => {
  azure.metadata.occupancy = 'testing'; azure.metadata.occupancyid = b.uuid(); azure.register(undefined, 'uncertain');
  await b.recover(azure, true, ['stop']);
  assert.equal(elapsed, 60); assert.equal(azure.metadata.resetstate, FENCED); assert.equal(azure.metadata.occupancy, 'released');
  assert.notEqual(azure.metadata.resetgeneration, generation); assert.deepEqual(azure.registry(), {});
  assert.equal('occupancyid' in azure.metadata, false);
});
for (const stopping of [false, true]) test(`recovery refuses ${stopping ? 'stop hold' : 'cleanup custody'} before stopping`, async () => {
  if (stopping) { azure.metadata.occupancy = 'stopping'; azure.metadata.occupancyid = b.uuid(); }
  else azure.metadata.resetstate = CLEANING;
  await failure(() => b.recover(azure, true, ['stop'])); assert.deepEqual(azure.events, []);
});
test('unproven recovery stop preserves exact authority', async () => {
  const before = { ...azure.metadata };
  mock.method(b.runtime, 'run_tree', async () => ({ returncode: 0, stdout: receipt(b.uuid()), stderr: '' }));
  await failure(() => b.recover(azure, true, ['stop'])); assert.deepEqual(azure.metadata, before);
});
for (const [label, change] of [
  ['resource ownership', (a: FakeAzure) => { a.tags.sandboxId = 'other'; }],
  ['unexpected database', (a: FakeAzure) => { a.databases.push({ name: 'foreign' }); }],
  ['unexpected container', (a: FakeAzure) => { a.containers.push({ name: 'foreign' }); }],
  ['missing container', (a: FakeAzure) => { a.containers.pop(); }],
  ['container ownership', (a: FakeAzure) => { a.data_metadata.sandboxid = 'foreign'; }],
  ['soft-deleted objects', (a: FakeAzure) => { a.items[a.c.data][0].deleted = true; }],
] as const) test(`inventory rejects ${label} without mutation`, async () => {
  change(azure); await failure(() => b.reset_inventory(azure)); assert.deepEqual(azure.events, []);
});
for (const settings of [{ isVersioningEnabled: true }, { deleteRetentionPolicy: { enabled: true } },
  { containerDeleteRetentionPolicy: { enabled: true } }, { restorePolicy: { enabled: true } }])
  test(`retained copies fail closed: ${JSON.stringify(settings)}`, async () => {
    azure.settings = settings; await failure(() => b.reset_inventory(azure)); assert.deepEqual(azure.events, []);
  });
for (const name of ['', '/foreign', '../foreign', 'table/../foreign', 'table\\foreign', './table'])
  test(`unscoped inventory path rejected: ${JSON.stringify(name)}`, async () => {
    azure.items[azure.c.data][0].name = name; await failure(() => b.reset_inventory(azure));
  });
test('ARM container metadata names compared case-insensitively', async () => {
  azure.data_metadata = { Sandboxid: azure.c.sandbox };
  azure.metadata.Sandboxid = azure.metadata.sandboxid; delete azure.metadata.sandboxid;
  await b.reset_inventory(azure);
});
test('testing occupancy prevents reset before stop or fence', async () => {
  azure.metadata.occupancy = 'testing'; azure.metadata.occupancyid = b.uuid();
  await failure(execute); assert.deepEqual(azure.events, []);
});
for (const result of [{ returncode: 1, stdout: '', stderr: 'stop failed' }, { returncode: 0, stdout: '', stderr: '' },
  { returncode: 0, stdout: receipt('00000000-0000-0000-0000-000000000000'), stderr: '' }])
  test(`unproven stop retains fence without deletion: ${JSON.stringify(result)}`, async () => {
    mock.method(b.runtime, 'run_tree', async () => result);
    await failure(execute);
    assert.equal(azure.metadata.resetstate, FENCED); assert.equal(azure.databases.length, 1);
    assert.equal(azure.items[azure.c.data].length, 1); assert.equal(elapsed, 0);
    assert.equal(fs.existsSync(recordPath), false);
  });
test('receipt parsing requires exact types/scope/preservation/generation amid noisy output', () => {
  assert.equal(b.verified_stop_receipt('noise\n' + receipt(generation), generation), true);
  for (const update of [{ stopped: 1 }, { scope: 'all' }, { dataPreserved: false }, { resetGeneration: b.uuid() }])
    assert.equal(b.verified_stop_receipt(JSON.stringify({ ...b.RECEIPT_FIELDS, resetGeneration: generation, ...update }), generation), false);
});
for (const state of ['Stopped', 'Stopping', 'Starting']) test(`cluster ${state} refuses reset before fencing`, async () => {
  azure.cluster_state = state; await failure(execute); assert.deepEqual(azure.events, []);
});
test('late publication during grace is inventoried and deleted', async () => {
  mock.method(b.runtime, 'sleep', async (seconds: number) => {
    elapsed += seconds; azure.items[azure.c.data].push({ name: 'late.parquet' });
  });
  await execute(); assert.equal(azure.items[azure.c.data].length, 0);
  assert.ok(azure.events.some(event => event[0] === 'DELETE' && event[1] === 'late.parquet'));
});
test('generation change during grace prevents all deletion', async () => {
  mock.method(b.runtime, 'sleep', async () => { azure.metadata.resetgeneration = b.uuid(); });
  await failure(execute); assert.equal(azure.databases.length, 1); assert.equal(fs.existsSync(recordPath), false);
});
test('stop completing during grace aborts fresh unused cleanup without deletion', async () => {
  mock.method(b.runtime, 'sleep', async () => { azure.cluster_state = 'Stopped'; });
  await assert.rejects(execute, /aborted before any deletion/);
  assert.equal(azure.metadata.resetstate, FENCED); assert.equal(azure.metadata.resetgeneration, generation);
  assert.deepEqual(azure.registry(), {}); assert.equal(azure.databases.length, 1); assert.equal(fs.existsSync(recordPath), false);
});
test('in-flight stop hold during grace refuses claim and does not delete', async () => {
  mock.method(b.runtime, 'sleep', async () => { azure.metadata.occupancy = 'stopping'; azure.metadata.occupancyid = b.uuid(); });
  await failure(execute); assert.equal(azure.databases.length, 1); assert.equal(fs.existsSync(recordPath), false);
});
test('files deleted before directories and directories deepest first', async () => {
  azure.directories[azure.c.data] = ['table', 'table/deep', 'empty', 'table/deep/deeper'];
  await execute();
  const deletes = azure.events.filter(event => ['DELETE', 'DELETE_DIRECTORY'].includes(event[0]));
  assert.equal(deletes[0][0], 'DELETE');
  assert.deepEqual(deletes.filter(event => event[0] === 'DELETE_DIRECTORY').map(event => event[1]),
    ['table/deep/deeper', 'table/deep', 'table', 'empty']);
});
test('failed physical emptiness retains cleaning authority and durable record', async () => {
  azure.blob = async () => [Buffer.alloc(0), {}];
  await assert.rejects(execute, /left workload state/);
  assert.equal(azure.metadata.resetstate, CLEANING); assert.equal(fs.existsSync(recordPath), true);
});
test('record is durable before claim and interruption after claim resumes exact identity', async () => {
  const original = azure.set_control.bind(azure); let operator = '';
  azure.set_control = async (metadata, etag) => {
    if (metadata.resetstate === CLEANING) {
      const record = b.read_reset_record(recordPath)!; operator = record.operator;
      assert.equal(record.generation, generation);
      assert.equal(fs.statSync(recordPath).mode & 0o777, 0o600);
      await original(metadata, etag); throw new b.Failure('lost claim');
    }
    return original(metadata, etag);
  };
  await assert.rejects(execute, /claim outcome unknown/);
  assert.deepEqual(Object.keys(azure.registry()), [operator]);
  azure.set_control = original; await resume(); assert.equal(fs.existsSync(recordPath), false);
});
test('ambiguous unapplied claim resumes with same persisted operator', async () => {
  const original = azure.set_control.bind(azure);
  azure.set_control = async (metadata, etag) => {
    if (metadata.resetstate === CLEANING) throw new b.Failure('request lost');
    return original(metadata, etag);
  };
  await failure(execute);
  const record = b.read_reset_record(recordPath)!;
  assert.equal(azure.metadata.resetstate, FENCED);
  azure.set_control = async (metadata, etag) => {
    if (metadata.resetstate === CLEANING) assert.deepEqual(Object.keys(JSON.parse(metadata.publishers)), [record.operator]);
    return original(metadata, etag);
  };
  await resume(); assert.equal(azure.metadata.resetgeneration, record.next);
});
test('applied object delete with lost response is observed absent, not replayed', async () => {
  const original = azure.blob.bind(azure); let deletes = 0;
  azure.blob = async (...args) => {
    const result = await original(...args);
    if (args[0] === 'DELETE') { deletes++; throw new b.Failure('response lost'); }
    return result;
  };
  await failure(execute); assert.equal(deletes, 1);
  azure.blob = original; await resume();
  assert.equal(azure.events.filter(event => event[0] === 'DELETE').length, 1);
});
test('unknown database submission is unresumable even after listing becomes absent', async () => {
  azure.delete_database = async () => { azure.databases = []; throw new b.Failure('response lost'); };
  await assert.rejects(execute, /UNRESUMABLE/);
  assert.equal(b.read_database_intent(recordPath, b.read_reset_record(recordPath)!)!.state, 'submitting');
  await assert.rejects(resume, /UNRESUMABLE/);
  assert.equal(azure.metadata.resetstate, CLEANING); assert.equal(azure.metadata.resetgeneration, generation);
});
test('crash during database submit cannot infer success from absent database', async () => {
  const record = await custody();
  b.publish_private(b.database_intent_path(recordPath), { operator: record.operator, generation, state: 'submitting', operation: null });
  azure.databases = []; await assert.rejects(resume, /UNRESUMABLE/); assert.equal(azure.metadata.resetstate, CLEANING);
});
for (const response of [[202, undefined], [202, 'https://foreign.invalid/operation'], [201, undefined]] as const)
  test(`database delete without trustworthy receipt retains submitting intent: ${JSON.stringify(response)}`, async () => {
    azure.delete_database = async () => [...response] as [number, string | undefined];
    await assert.rejects(execute, /UNRESUMABLE/);
    assert.equal(b.read_database_intent(recordPath, b.read_reset_record(recordPath)!)!.state, 'submitting');
  });
test('accepted database operation polls until explicit provider success before rotating', async () => {
  let polls = 0;
  azure.delete_database = async () => [202, 'https://management.azure.com/operations/one?t=signed'];
  azure.provider_operation = async () => {
    polls++; if (polls === 1) return 'InProgress'; azure.databases = []; return 'Succeeded';
  };
  await execute(); assert.equal(polls, 2); assert.equal(elapsed, 70);
  assert.equal(azure.metadata.resetstate, FENCED); assert.notEqual(azure.metadata.resetgeneration, generation);
});
for (const status of ['Failed', 'Canceled']) test(`terminal provider ${status} permits one later resubmission`, async () => {
  let submits = 0;
  azure.delete_database = async () => {
    submits++; if (submits === 1) return [202, 'https://management.azure.com/operations/one'];
    azure.databases = []; return [204, undefined];
  };
  azure.provider_operation = async () => status;
  await assert.rejects(execute, /resume may resubmit/);
  assert.equal(fs.existsSync(b.database_intent_path(recordPath)), false); assert.equal(fs.existsSync(recordPath), true);
  await resume(); assert.equal(submits, 2); assert.equal(fs.existsSync(recordPath), false);
});
test('unknown provider receipt retains accepted intent and never resubmits', async () => {
  let submits = 0;
  azure.delete_database = async () => { submits++; return [202, 'https://management.azure.com/operations/one']; };
  azure.provider_operation = async () => 'Unknown';
  await assert.rejects(execute, /provider receipt is unknown/); await assert.rejects(resume, /provider receipt is unknown/);
  assert.equal(submits, 1);
  assert.equal(b.read_database_intent(recordPath, b.read_reset_record(recordPath)!)!.state, 'accepted');
});
test('preexisting Deleting database without own intent refuses submission', async () => {
  azure.databases[0].properties = { provisioningState: 'Deleting' };
  await assert.rejects(execute, /Deleting without/); assert.ok(!azure.events.some(event => event[0] === 'arm-delete-db'));
});
test('foreign intent and tampered accepted operation cannot be adopted', async () => {
  const record = await custody();
  b.publish_private(b.database_intent_path(recordPath), { operator: b.uuid(), generation, state: 'accepted', operation: 'https://management.azure.com/operations/one' });
  await assert.rejects(resume, /other custody/);
  fs.unlinkSync(b.database_intent_path(recordPath));
  b.publish_private(b.database_intent_path(recordPath), { operator: record.operator, generation, state: 'accepted', operation: 'https://evil.invalid/operations' });
  await assert.rejects(resume, /not an ARM operation/); assert.ok(!azure.events.some(event => event[0] === 'arm-operation'));
});
test('ambiguous finish reconciles only exact persisted successor without deleting again', async () => {
  const original = azure.set_control.bind(azure);
  azure.set_control = async (metadata, etag) => {
    await original(metadata, etag);
    if (metadata.resetstate === FENCED && metadata.resetgeneration !== generation) throw new b.Failure('finish response lost');
  };
  await failure(execute);
  const record = b.read_reset_record(recordPath)!;
  assert.equal(azure.metadata.resetgeneration, record.next);
  const before = azure.events.length;
  azure.set_control = original; await resume(); assert.equal(azure.events.length, before); assert.equal(fs.existsSync(recordPath), false);
});
for (const mismatch of ['foreign member', 'uncertain member', 'generation', 'sandbox', 'noncanonical record'])
  test(`resume never adopts ${mismatch}`, async () => {
    const record = await custody();
    if (mismatch === 'foreign member') azure.metadata.publishers = b.canonical_json({ [b.uuid()]: { generation, state: 'active' } });
    if (mismatch === 'uncertain member') azure.metadata.publishers = b.canonical_json({ [record.operator]: { generation, state: 'uncertain' } });
    if (mismatch === 'generation') { azure.metadata.resetgeneration = b.uuid(); azure.metadata.publishers = '{}'; }
    if (mismatch === 'sandbox') fs.writeFileSync(recordPath, b.record_bytes({ ...record, sandbox: 'foreign' }));
    if (mismatch === 'noncanonical record') fs.writeFileSync(recordPath, JSON.stringify(record, null, 2));
    const before = azure.events.length; await failure(resume); assert.equal(azure.events.length, before); assert.equal(fs.existsSync(recordPath), true);
  });
test('record publishing is canonical fsync-file then parent and create-if-absent never overwrites', () => {
  const record = makeRecord(); const order: string[] = [];
  const original = fs.fsyncSync;
  mock.method(fs, 'fsyncSync', (fd: number) => { order.push(fs.fstatSync(fd).isDirectory() ? 'directory' : 'file'); return original(fd); });
  b.write_reset_record(recordPath, record);
  assert.deepEqual(order, ['file', 'directory']); assert.deepEqual(fs.readFileSync(recordPath), b.record_bytes(record));
  assert.throws(() => b.write_reset_record(recordPath, makeRecord()), /never overwriting/);
  assert.deepEqual(fs.readFileSync(recordPath), b.record_bytes(record));
});
test('record owner compare protects replacement/removal against TOCTOU losers', () => {
  const winner = makeRecord(), loser = makeRecord(); b.write_reset_record(recordPath, winner);
  assert.throws(() => b.publish_private(recordPath, loser, loser), /never overwriting/);
  assert.throws(() => b.remove_reset_record(recordPath, loser), /never removing/);
  assert.deepEqual(fs.readFileSync(recordPath), b.record_bytes(winner));
});
test('directory fsync failure occurs before claim, resume repairs durability', async () => {
  const original = fs.fsyncSync;
  const patched = mock.method(fs, 'fsyncSync', (fd: number) => {
    if (fs.fstatSync(fd).isDirectory()) throw new Error('fsync failure');
    return original(fd);
  });
  await assert.rejects(execute, /made durable/);
  assert.equal(azure.metadata.resetstate, FENCED); assert.deepEqual(azure.registry(), {});
  assert.equal(fs.existsSync(recordPath), true);
  patched.mock.restore(); await resume(); assert.equal(fs.existsSync(recordPath), false);
});
test('kernel local lock excludes second reset throughout custody, releases on close', async () => {
  await b.reset_lock(recordPath, async () => {
    const second = spawnSync('flock', ['-n', recordPath + '.lock', 'true']);
    assert.equal(second.status, 1);
    await assert.rejects(() => b.reset_lock(recordPath, async () => {}), /local custody lock/);
  });
  const after = spawnSync('flock', ['-n', recordPath + '.lock', 'true']);
  assert.equal(after.status, 0);
});
test('definite claim loser never removes a replaced foreign record', async () => {
  const original = azure.set_control.bind(azure); let winner: b.ResetRecord;
  azure.set_control = async (metadata, etag) => {
    if (metadata.resetstate === CLEANING) {
      winner = makeRecord(); fs.writeFileSync(recordPath, b.record_bytes(winner));
      throw new b.Failure('claim lost');
    }
    return original(metadata, etag);
  };
  await failure(execute); assert.deepEqual(b.read_reset_record(recordPath), winner!);
});
test('unapplied object timeout reissues only exact inventoried ETag; successor cannot be deleted', async () => {
  const original = azure.blob.bind(azure);
  azure.blob = async (...args) => { if (args[0] === 'DELETE') throw new b.Failure('timeout'); return original(...args); };
  await failure(execute);
  azure.items[azure.c.data][0].etag = '"successor"';
  azure.blob = original;
  await assert.rejects(() => b.delete_inventoried(azure, azure.c.data, { name: 'table/data.parquet', etag: '"table/data.parquet"' }), b.HttpFailure);
  assert.equal(azure.items[azure.c.data].length, 1);
  await resume(); assert.equal(azure.items[azure.c.data].length, 0);
});
test('missing ETag prevents unconditional object deletion', async () => {
  azure.items[azure.c.data][0].etag = null;
  await assert.rejects(execute, /no ETag/); assert.ok(!azure.events.some(event => ['DELETE', 'arm-delete-db'].includes(event[0])));
});
test('immutable version/snapshot deletion uses immutable identity and no mutable-path fallback', async () => {
  const calls: any[][] = [];
  azure.blob = async (...args) => { calls.push(args); return [Buffer.alloc(0), {}]; };
  await b.delete_inventoried(azure, azure.c.data, { name: 'x', version: 'v', snapshot: 's' });
  await b.delete_inventoried(azure, azure.c.data, { name: 'x', snapshot: 's' });
  assert.deepEqual(calls.map(call => call[3]), [{ versionid: 'v' }, { snapshot: 's' }]);
});
test('bounded parallelism awaits started work and cancels unstarted work after failure', async () => {
  let active = 0, maximum = 0, finished = 0;
  await b.parallel_each(async () => { active++; maximum = Math.max(active, maximum); await Promise.resolve(); active--; finished++; }, Array.from({ length: 100 }, (_, i) => i));
  assert.equal(maximum, b.DELETE_PARALLELISM); assert.equal(finished, 100);
  active = 0; finished = 0;
  await assert.rejects(() => b.parallel_each(async item => {
    active++; await Promise.resolve(); active--; finished++;
    if (item === 0) throw new b.Failure('delete failed');
  }, Array.from({ length: 100 }, (_, i) => i)), /delete failed/);
  assert.equal(active, 0); assert.ok(finished <= b.DELETE_PARALLELISM);
});

test('occupy exports captured generation privately, not the run UUID', async () => {
  const owner = b.uuid(); await b.occupancy(azure, 'occupy', owner, output);
  assert.equal(azure.metadata.occupancy, 'testing'); assert.equal(b.load_env(output).ARROWCOL_RESET_GENERATION, generation);
  assert.notEqual(generation, owner); assert.equal(fs.statSync(output).mode & 0o777, 0o600);
});
test('generation change after occupy CAS prevents exporting admission', async () => {
  const original = azure.set_control.bind(azure);
  azure.set_control = async (metadata, etag) => { await original(metadata, etag); azure.metadata.resetgeneration = b.uuid(); };
  await failure(() => b.occupancy(azure, 'occupy', b.uuid(), output)); assert.equal(fs.existsSync(output), false);
});
test('only exact owner and generation with empty registry can release testing occupancy', async () => {
  const owner = b.uuid(); await b.occupancy(azure, 'occupy', owner);
  await failure(() => b.occupancy(azure, 'release', b.uuid(), undefined, generation));
  await failure(() => b.occupancy(azure, 'release', owner));
  await failure(() => b.occupancy(azure, 'release', owner, undefined, b.uuid()));
  const member = azure.register();
  await failure(() => b.occupancy(azure, 'release', owner, undefined, generation));
  await new PublisherControl(azure).clean_release(member, generation, true);
  await b.occupancy(azure, 'release', owner, undefined, generation); assert.equal(azure.metadata.occupancy, 'released');
});
test('occupy before stop blocks ARM stop, same owner may reoccupy', async () => {
  const owner = b.uuid(); await b.occupancy(azure, 'occupy', owner); await b.occupancy(azure, 'occupy', owner);
  await failure(() => b.stop_cluster(azure)); assert.equal(azure.events.filter(event => event[0] === 'cli').length, 0);
});
test('stop hold excludes tests and publishers until confirmed stop releases it', async () => {
  const original = azure.cli.bind(azure);
  azure.cli = async (...args) => {
    assert.equal(azure.metadata.occupancy, 'stopping');
    await failure(() => b.occupancy(azure, 'occupy', b.uuid()));
    await failure(() => new PublisherControl(azure).admit(b.uuid(), generation));
    return original(...args);
  };
  await b.stop_cluster(azure); assert.equal(azure.cluster_state, 'Stopped'); assert.equal(azure.metadata.occupancy, 'released');
});
test('occupy racing stop CAS prevents any ARM stop', async () => {
  const original = azure.set_control.bind(azure);
  azure.set_control = async (metadata, etag) => {
    azure.set_control = original;
    await b.occupancy(azure, 'occupy', b.uuid());
    await original(metadata, etag);
  };
  await failure(() => b.stop_cluster(azure)); assert.equal(azure.events.filter(event => event[0] === 'cli').length, 0);
});
test('unknown ARM stop retains blocking hold without replay or release', async () => {
  azure.cli = async () => { throw new b.Failure('ARM timeout'); };
  await failure(() => b.stop_cluster(azure));
  const hold = azure.metadata.occupancyid;
  await failure(() => b.occupancy(azure, 'occupy', b.uuid()));
  await failure(() => b.occupancy(azure, 'release', hold, undefined, generation));
  await assert.rejects(() => b.stop_cluster(azure), /ambiguous/);
  assert.equal(azure.metadata.occupancy, 'stopping'); assert.equal(azure.metadata.occupancyid, hold);
});
test('unknown stop wait retains hold and definite CAS conflict stays retryable', async () => {
  const original = azure.set_control.bind(azure);
  azure.set_control = async () => { throw new b.ControlConflict('busy'); };
  await failure(() => b.stop_cluster(azure)); assert.equal(azure.metadata.occupancy, 'released'); assert.deepEqual(azure.events, []);
  azure.set_control = original;
  azure.cli = async () => {};
  await failure(() => b.stop_cluster(azure)); assert.equal(azure.metadata.occupancy, 'stopping');
});
test('fenced sandbox cannot be occupied and stale ETag cannot write', async () => {
  azure.metadata.resetstate = FENCED; await failure(() => b.occupancy(azure, 'occupy', b.uuid()));
  const [metadata, etag] = await azure.control(); azure.etag = '"concurrent"';
  await failure(() => azure.set_control(metadata, etag));
});

test('missing Delta root returns NotReady before all ARM/KQL', async () => {
  azure.arm = async () => { throw new Error('unexpected ARM'); };
  await assert.rejects(() => b.reconcile(azure, manifest), b.NotReady);
  assert.equal(azure.events.filter(event => event[0] === 'kql').length, 0);
});
test('wait retries only missing logs, never authentication failure', async () => {
  azure.blob = async () => { throw new b.HttpFailure(403, 'denied'); };
  await assert.rejects(() => b.reconcile(azure, manifest, 30), b.HttpFailure); assert.equal(elapsed, 0);
});
test('missing log becomes ready within bounded wait', async () => {
  mock.method(b.runtime, 'sleep', async (seconds: number) => { elapsed += seconds; initialize(); });
  await b.reconcile(azure, manifest, 10); assert.equal(elapsed, 1);
  assert.equal(azure.events.filter(event => event[0] === 'kql').length, 4);
});
for (const tables of [[], {}, ['bad\'); .drop database x'], ['Logs', 'Logs'],
  [{ name: 'Logs', relative_path: '../foreign' }], [{ name: 'Logs', relative_path: '/absolute' }],
  [{ name: 'Logs', relative_path: 'https://foreign' }], [{ name: 'Logs', relative_path: 'table;token=secret' }],
  [{ name: 'Logs', relative_path: 'table', extra: true }], [{ name: 1, relative_path: 'table' }]])
  test(`unsafe manifest fails before I/O: ${JSON.stringify(tables)}`, async () => {
    fs.writeFileSync(manifest, JSON.stringify(tables)); await failure(() => b.reconcile(azure, manifest)); assert.deepEqual(azure.events, []);
  });
for (const log of ['not-json', '{"protocol":{}}', '{"metaData":{}}', '{}'])
  test(`invalid initialization log blocks KQL: ${log}`, async () => {
    azure.logs.set('Logs/_delta_log/00000000000000000000.json', Buffer.from(log));
    await failure(() => b.reconcile(azure, manifest)); assert.equal(azure.events.filter(event => event[0] === 'kql').length, 0);
  });
test('policy order uses exact managed-identity usage and inferred schema before QAP', async () => {
  initialize(); await b.reconcile(azure, manifest);
  const commands = azure.events.filter(event => event[0] === 'kql').map(event => event[1]);
  assert.match(commands[0], /ExternalTable,AutomatedFlows/); assert.match(commands[1], /^\.show external tables/);
  assert.match(commands[2], /create-or-alter external table.*kind=delta/);
  assert.match(commands[3], /query_acceleration.*"ManagedIdentity"/);
  assert.ok(!commands[0].includes('QueryAcceleration'));
});
for (const repoint of [true, false]) test(`${repoint ? 'repointed' : 'same-location'} accelerated table policy ordering`, async () => {
  initialize();
  const original = azure.kql.bind(azure);
  azure.kql = async command => {
    await original(command);
    if (command.startsWith('.show external tables')) return { Tables: [{ Rows: [['Logs', JSON.stringify([
      `abfss://${azure.c.data}@${azure.c.account}.dfs.core.windows.net/${repoint ? 'old/' : ''}Logs;managed_identity=${SUB}`,
    ])]] }] };
    return {};
  };
  await b.reconcile(azure, manifest);
  const commands = azure.events.filter(event => event[0] === 'kql').map(event => event[1]);
  const deletion = commands.findIndex(command => command.startsWith('.delete external table'));
  assert.equal(deletion >= 0, repoint);
  if (repoint) assert.ok(deletion < commands.findIndex(command => command.includes('.create-or-alter')));
  assert.match(commands.at(-1), /query_acceleration/);
});
test('both pools use catalog paths, inferred schema and isolated root prefix', async () => {
  azure.c.table_prefix = 'loadtest/run-example';
  const bindings = ['direct', 'dataflow'].map(pool => ({ name: pool + '_table', relative_path: pool + '/telemetry/sqlserver/table' }));
  fs.writeFileSync(manifest, JSON.stringify(bindings));
  for (const item of bindings) azure.logs.set(`loadtest/run-example/${item.relative_path}/_delta_log/00000000000000000000.json`,
    Buffer.from('{"protocol":{}}\n{"metaData":{}}'));
  await b.reconcile(azure, manifest);
  const commands = azure.events.filter(event => event[0] === 'kql').map(event => event[1]);
  assert.equal(commands.length, 6);
  assert.match(commands[2], /\['direct_table'\] kind=delta.*\/loadtest\/run-example\/direct\/telemetry\/sqlserver\/table;/);
  assert.match(commands[4], /\['dataflow_table'\] kind=delta/);
});

function leasedStorage() {
  const real = new b.Azure(azure.c);
  const state = { metadata: { ...azure.metadata }, lease: null as string | null, calls: [] as any[], etag: 1 };
  real.blob = async (method, _container, _name = '', query = {}, _body, headers = {}) => {
    assert.ok(!('If-Match' in headers));
    const action = headers['x-ms-lease-action'];
    state.calls.push(action || [method, query.comp ?? null]);
    if (method === 'HEAD') return [Buffer.alloc(0), { ...Object.fromEntries(Object.entries(state.metadata)
      .map(([k, v]) => ['x-ms-meta-' + k, v])), etag: `"${state.etag}"` }];
    if (action === 'acquire') {
      if (state.lease) throw new b.HttpFailure(409, 'LeaseAlreadyPresent');
      assert.equal(headers['x-ms-lease-duration'], String(b.LEASE_SECONDS));
      state.lease = headers['x-ms-proposed-lease-id']; return [Buffer.alloc(0), {}];
    }
    if (action === 'release') {
      if (headers['x-ms-lease-id'] === state.lease) state.lease = null;
      return [Buffer.alloc(0), {}];
    }
    if (query.comp === 'metadata') {
      if (!state.lease || headers['x-ms-lease-id'] !== state.lease) throw new b.HttpFailure(412, 'LeaseIdMismatch');
      state.metadata = Object.fromEntries(Object.entries(headers).filter(([k]) => k.startsWith('x-ms-meta-'))
        .map(([k, v]) => [k.slice(10), v]));
      state.etag++; return [Buffer.alloc(0), {}];
    }
    throw new Error('unexpected storage operation');
  };
  return { real, state };
}
test('real metadata CAS leases then compares full prior content, writes all metadata, releases', async () => {
  const { real, state } = leasedStorage(), [metadata, etag] = await real.control();
  metadata.resetstate = FENCED;
  await real.set_control(metadata, etag);
  assert.deepEqual(state.calls, [['HEAD', null], 'acquire', ['HEAD', null], ['PUT', 'metadata'], 'release']);
  assert.equal(state.metadata.custom, 'preserved'); assert.equal(state.metadata.resetstate, FENCED); assert.equal(state.lease, null);
});
test('busy metadata lease performs no write', async () => {
  const { real, state } = leasedStorage(), [metadata, etag] = await real.control();
  state.lease = b.uuid();
  await assert.rejects(() => real.set_control({ ...metadata, resetstate: FENCED }, etag), b.ControlConflict);
  assert.equal(state.metadata.resetstate, READY); assert.ok(!state.calls.some(call => Array.isArray(call) && call[1] === 'metadata'));
});
test('concurrent metadata change after original read refuses write and releases lease', async () => {
  const { real, state } = leasedStorage(), [metadata, etag] = await real.control();
  state.metadata.occupancy = 'testing';
  await assert.rejects(() => real.set_control({ ...metadata, resetstate: FENCED }, etag), b.ControlConflict);
  assert.equal(state.calls.at(-1), 'release'); assert.equal(state.lease, null); assert.equal(state.metadata.resetstate, READY);
});
test('lost lease HTTP 412 is definite conflict and unknown write failure is not', async () => {
  for (const ambiguous of [false, true]) {
    const { real, state } = leasedStorage(), original = real.blob.bind(real);
    const [metadata, etag] = await real.control();
    real.blob = async (...args) => {
      if (args[3]?.comp === 'metadata') {
        if (ambiguous) { await original(...args); throw new b.Failure('timeout'); }
        state.lease = b.uuid();
      }
      return original(...args);
    };
    await assert.rejects(() => real.set_control({ ...metadata, resetstate: FENCED }, etag),
      error => error instanceof b.Failure && (ambiguous ? !(error instanceof b.ControlConflict) : error instanceof b.ControlConflict));
    assert.equal(state.calls.at(-1), 'release'); assert.equal(state.metadata.resetstate, ambiguous ? FENCED : READY);
  }
});
test('metadata mutation without prior read is refused before lease', async () => {
  const { real, state } = leasedStorage();
  await assert.rejects(() => real.set_control({ resetstate: FENCED }, '"unread"'), b.ControlConflict);
  assert.deepEqual(state.calls, []);
});
for (const missing of ['sandboxid', 'resetstate', 'resetgeneration', 'occupancy', 'publisherregistryversion', 'publishers', 'etag'])
  test(`real authority rejects missing ${missing}`, async () => {
    const { real, state } = leasedStorage();
    if (missing === 'etag') {
      const original = real.blob.bind(real);
      real.blob = async (...args) => { const [data, headers] = await original(...args); delete headers.etag; return [data, headers]; };
    } else delete state.metadata[missing];
    await failure(() => real.control());
  });
test('blob inventory paginates retained copies and decodes names/ETags exactly', async () => {
  const real = new b.Azure(azure.c), calls: any[][] = [];
  real.blob = async (...args) => {
    calls.push(args);
    const xml = calls.length === 1 ?
      '<EnumerationResults><Blobs><Blob><Name>one&amp;two</Name><Snapshot>s</Snapshot><Properties><Etag>&quot;etag&quot;</Etag></Properties></Blob></Blobs><NextMarker>next</NextMarker></EnumerationResults>' :
      '<EnumerationResults><Blobs><Blob><Name>two</Name><VersionId>v</VersionId><Deleted>true</Deleted></Blob></Blobs><NextMarker/></EnumerationResults>';
    return [Buffer.from(xml), {}];
  };
  const blobs = await real.blobs(azure.c.data);
  assert.equal(blobs[0].name, 'one&two'); assert.equal(blobs[0].etag, '"etag"'); assert.equal(blobs[0].snapshot, 's');
  assert.equal(blobs[1].version, 'v'); assert.equal(blobs[1].deleted, true);
  assert.equal(calls[0][3].include, 'snapshots,versions,deleted,metadata'); assert.equal(calls[1][3].marker, 'next');
});
test('HNS path inventory paginates empty directories', async () => {
  const real = new b.Azure(azure.c); let calls = 0;
  real.request = async (_method, url) => {
    calls++;
    if (calls === 2) assert.equal(new URL(url).searchParams.get('continuation'), 'more');
    return [Buffer.from(JSON.stringify({ paths: [{ name: calls === 1 ? 'one' : 'empty', isDirectory: calls === 2 }] })),
      calls === 1 ? { 'x-ms-continuation': 'more' } : {} as Record<string, string>];
  };
  assert.deepEqual((await real.paths(azure.c.data)).map(item => item.name), ['one', 'empty']); assert.equal(calls, 2);
});
test('ARM pagination rejects foreign scope before authenticated follow-up', async () => {
  const real = new b.Azure(azure.c);
  real.arm = async () => ({ value: [], nextLink: 'https://foreign.invalid/?token=secret' });
  real.request = async () => { throw new Error('unsafe request'); };
  await assert.rejects(() => real.collection(azure.c.cluster_id + '/databases'), /Unexpected ARM pagination/);
});
test('OAuth storage transport sends Azure date, version and token without logging it', async () => {
  const real = new b.Azure(azure.c);
  real.token = async () => 'test-token';
  mock.method(globalThis, 'fetch', async (_url: string | URL | Request, init?: RequestInit) => {
    const headers = init!.headers as Record<string, string>;
    assert.ok(headers['x-ms-date']); assert.equal(headers['x-ms-version'], '2023-11-03');
    assert.equal(headers.Authorization, 'Bearer test-token');
    return new Response('', { status: 200 });
  });
  await real.blob('HEAD', azure.c.checkpoints, '', { restype: 'container' });
});
test('parallel token refresh is shared and bounded by cache lifetime', async () => {
  const real = new b.Azure(azure.c); let calls = 0;
  real.cli = async () => { calls++; await Promise.resolve(); return { accessToken: 'token' + calls }; };
  const tokens = await Promise.all(Array.from({ length: 32 }, () => real.token('resource')));
  assert.equal(new Set(tokens).size, 1); assert.equal(calls, 1);
  elapsed = 241; assert.equal(await real.token('resource'), 'token2'); assert.equal(calls, 2);
});
test('GET/HEAD transient failures retry boundedly; writes/nontransient reads remain single-shot', async () => {
  for (const method of ['GET', 'HEAD', 'DELETE', 'PUT', 'POST']) for (const code of [403, 429, 504]) {
    const real = new b.Azure(azure.c); let calls = 0;
    real._request_once = async () => { calls++; throw new b.HttpFailure(code, 'unavailable'); };
    await failure(() => real.request(method, 'https://unused', 'resource'));
    assert.equal(calls, ['GET', 'HEAD'].includes(method) && code !== 403 ? b.READ_ATTEMPTS : 1);
  }
});
test('read retries gateway timeout then succeeds and network retry trail stays bounded', async () => {
  const real = new b.Azure(azure.c); let calls = 0;
  real._request_once = async () => {
    calls++; if (calls < 3) throw new b.HttpFailure(504, 'gateway'); return [Buffer.from('ok'), {}];
  };
  assert.equal((await real.request('GET', 'unused', 'resource'))[0]!.toString(), 'ok'); assert.equal(calls, 3);
  calls = 0; real._request_once = async () => { calls++; throw new b.Failure('network'); };
  await failure(() => real.request('GET', 'unused', 'resource')); assert.equal(calls, b.READ_ATTEMPTS);
});
test('real DELETE captures status and only Azure-AsyncOperation provider receipt', async () => {
  const real = new b.Azure(azure.c);
  real.request = async () => [Buffer.alloc(0), { ':status': '202', 'azure-asyncoperation': 'https://management.azure.com/operations/one' }];
  assert.deepEqual(await real.delete_database(), [202, 'https://management.azure.com/operations/one']);
  real.request = async () => [Buffer.alloc(0), { ':status': '202', location: 'https://management.azure.com/operations/one' }];
  assert.deepEqual(await real.delete_database(), [202, undefined]);
  real.request = async () => [null, {}]; assert.deepEqual(await real.delete_database(), [404, undefined]);
});
for (const unsafe of ['http://management.azure.com/operations/x', 'https://foreign.invalid/operation',
  'https://management.azure.com:443/operations/x', 'https://user@management.azure.com/operations/x',
  'https://management.azure.com/../x', 'https://management.azure.com/operation#fragment',
  'https://management.azure.com\\foreign/x', 'https://management.azure.com', 'https://management.azure.com/x\n',
  null, 'https://management.azure.com/x?' + 'a'.repeat(b.MAX_OPERATION_URL)])
  test(`operation URL rejected before any OAuth: ${String(unsafe).slice(0, 100)}`, async () => {
    const real = new b.Azure(azure.c);
    real.token = async () => { throw new Error('token fetched before URL validated'); };
    await failure(() => real.provider_operation(unsafe as string));
  });
test('signed multi-kilobyte ARM operation accepted; explicit provider statuses only', async () => {
  const operation = 'https://management.azure.com/subscriptions/x/operations/y?t=' + 'a'.repeat(3500) + '&c=a%2Bb&s=x&h=y';
  assert.equal(b.operation_url(operation), operation);
  const real = new b.Azure(azure.c);
  for (const [status, expected] of [['Succeeded', 'Succeeded'], ['running', 'InProgress'], ['Accepted', 'InProgress'],
    ['Deleting', 'InProgress'], ['Failed', 'Failed'], ['Canceled', 'Canceled'], ['Unknown', 'Unknown']]) {
    real.request = async () => [Buffer.from(JSON.stringify({ status })), { ':status': '200' }];
    assert.equal(await real.provider_operation(operation), expected);
  }
  for (const body of ['{}', 'null', '[]', 'not-json', '{"status":1}']) {
    real.request = async () => [Buffer.from(body), { ':status': '200' }];
    assert.equal(await real.provider_operation(operation), 'Unknown');
  }
  real.request = async () => [Buffer.from('{"status":"Succeeded"}'), { ':status': '202' }];
  assert.equal(await real.provider_operation(operation), 'Unknown');
});
test('initializer hook calls only owned Rust command and requires every private input', async () => {
  const cargo = path.join(directory, 'cargo');
  fs.writeFileSync(cargo, '#!/bin/sh\nprintf "%s\\n" "$@"\n', { mode: 0o700 });
  const env = b.initializer_env({}, azure.c, manifest, generation);
  env.PATH = directory + ':' + process.env.PATH;
  const hook = path.join(b.ROOT, 'infra/azure/initialize-delta.sh');
  const result = await realRun(['bash', hook], 5, b.ROOT, env);
  assert.equal(result.returncode, 0, result.stderr);
  assert.deepEqual(result.stdout.trim().split('\n'), ['run', '--locked', '--quiet', '--package', 'arrowcol-loadgen',
    '--', 'initialize-azure', '--manifest', manifest]);
  for (const missing of ['ARROWCOL_AZURE_TABLE_MANIFEST', 'ARROWCOL_AZURE_TABLE_ROOT', 'AZURE_STORAGE_ACCOUNT',
    'AZURE_STORAGE_CONTAINER', 'AZURE_STORAGE_CHECKPOINT_CONTAINER', 'ARROWCOL_EXPECTED_RESET_GENERATION']) {
    const incomplete = { ...env }; delete incomplete[missing];
    const result = await realRun(['bash', hook], 5, b.ROOT, incomplete);
    assert.notEqual(result.returncode, 0); assert.match(result.stderr, /bootstrap must provide/); assert.equal(result.stdout, '');
  }
});
test('timeout kills complete process group and reports unknown outcome', async () => {
  const marker = path.join(directory, 'child-survived');
  await assert.rejects(() => realRun(['bash', '-c', `(sleep .8; touch '${marker}') & sleep 30`], 0.1), /outcome unknown/);
  await new Promise(resolve => setTimeout(resolve, 1000)); assert.equal(fs.existsSync(marker), false);
});
test('outer SIGINT kills group and surfaces blocking unknown outcome', async () => {
  const marker = path.join(directory, 'child-survived');
  const timer = setTimeout(() => process.emit('SIGINT'), 100);
  try { await assert.rejects(() => realRun(['bash', '-c', `(sleep .8; touch '${marker}') & sleep 30`], 5), /interrupted.*outcome unknown/); }
  finally { clearTimeout(timer); }
  await new Promise(resolve => setTimeout(resolve, 1000)); assert.equal(fs.existsSync(marker), false);
});
test('stop command timeout kills tree and leaves reset fenced with no deletion', async () => {
  const marker = path.join(directory, 'stop-child-survived');
  mock.method(b.runtime, 'run_tree', async (command: string[]) => realRun(command, 0.1));
  await assert.rejects(() => b._unqualified_reset_sequence(azure,
    ['bash', '-c', `(sleep .8; touch '${marker}') & sleep 30`], recordPath), /outcome unknown/);
  await new Promise(resolve => setTimeout(resolve, 1000));
  assert.equal(fs.existsSync(marker), false); assert.equal(azure.metadata.resetstate, FENCED); assert.equal(azure.databases.length, 1);
});
test('Bicep separates Kusto client application identity from storage object principal and preserves live authority', () => {
  const source = fs.readFileSync(path.join(b.ROOT, 'infra/azure/sandbox.bicep'), 'utf8');
  assert.match(source.split('resource reader ')[1], /principalId: identity\.properties\.clientId/);
  assert.match(source.split('resource readData ')[1].split('resource writeData ')[0], /principalId: identity\.properties\.principalId/);
  assert.match(source.split('resource checkpoints ')[1].split('resource readData ')[0], /if \(!checkpointExists\)/);
});
function coordinateProvision() {
  const deployed: any[] = [], observed: any[] = [], child: { generation?: string; id?: string; then?: () => Promise<void> } = {};
  const originalCli = azure.cli.bind(azure), originalKql = azure.kql.bind(azure);
  azure.cli = async (...args) => {
    if (args.slice(0, 3).join(' ') === 'deployment sub create') {
      const params = JSON.parse(fs.readFileSync(args.at(-1)!.slice(1), 'utf8')).parameters;
      deployed.push([params.checkpointExists.value, params.controlMetadata.value,
        azure.checkpoint_exists ? azure.registry() : null, params]);
      if (!params.checkpointExists.value) {
        azure.metadata = { ...params.controlMetadata.value, sandboxid: azure.c.sandbox, project: 'rust-playground' };
        azure.checkpoint_exists = true;
      }
      azure.events.push(['arm', 'deploy']); return {};
    }
    observed.push([args.some(arg => arg.includes('/start?')) ? 'start' : 'stop', azure.metadata.resetstate, azure.registry()]);
    return originalCli(...args);
  };
  azure.kql = async command => {
    observed.push(['kql', azure.metadata.resetstate, azure.registry()]); return originalKql(command);
  };
  mock.method(b.runtime, 'run_tree', async (_command: string[], _timeout: number, _cwd?: string, env?: NodeJS.ProcessEnv) => {
    const control = new PublisherControl(azure), member = b.uuid(), gen = env!.ARROWCOL_EXPECTED_RESET_GENERATION!;
    child.id = member; child.generation = gen;
    await control.admit(member, gen, true); await control.clean_release(member, gen, true);
    if (child.then) await child.then();
    return { returncode: 0, stdout: '', stderr: '' };
  });
  return { deployed, observed, child };
}
test('fresh provisioning initializes v1 then child/fresh parent serialize start and all KQL', async () => {
  initialize(); azure.checkpoint_exists = false; azure.cluster_state = 'Stopped';
  const { deployed, observed, child } = coordinateProvision();
  await b.provision(azure, manifest, 0, output, false, {});
  const [exists, fresh] = deployed[0];
  assert.equal(exists, false); assert.equal(fresh.resetstate, FENCED); assert.equal(fresh.publishers, '{}');
  assert.equal(fresh.occupancy, 'released'); assert.equal(fresh.publisherregistryversion, '1');
  assert.equal(child.generation, fresh.resetgeneration);
  for (const [operation, state, registry] of observed) {
    assert.equal(state, FENCED, operation); assert.equal(Object.keys(registry).length, 1); assert.ok(!(child.id! in registry));
  }
  assert.equal(azure.metadata.resetstate, READY); assert.deepEqual(azure.registry(), {});
  assert.equal(b.load_env(output).ARROWCOL_RESET_GENERATION, fresh.resetgeneration);
});
test('ready redeploy preserves all authority metadata while exclusive member owns deploy/start/KQL', async () => {
  initialize(); const before = { ...azure.metadata }, { deployed, observed, child } = coordinateProvision();
  await b.provision(azure, manifest, 0, output, false, {});
  assert.equal(deployed[0][0], true); assert.deepEqual(deployed[0][1], {}); assert.equal(Object.keys(deployed[0][2]).length, 1);
  assert.deepEqual(azure.metadata, before); assert.equal(child.id, undefined);
  for (const [, state, registry] of observed) { assert.equal(state, FENCED); assert.equal(Object.keys(registry).length, 1); }
  assert.equal(b.load_env(output).ARROWCOL_RESET_GENERATION, generation);
});
test('infra-only deliberately omits initializer/QAP and does not declare fresh fenced authority ready', async () => {
  azure.metadata.resetstate = FENCED; const { child } = coordinateProvision();
  await b.provision(azure, manifest, 0, output, true, {});
  assert.equal(child.id, undefined); assert.equal(azure.metadata.resetstate, FENCED);
  assert.equal(b.load_env(output).ARROWCOL_RESET_GENERATION, undefined);
});
for (const state of [READY, FENCED]) test(`terminal deployment failure releases ${state} custody`, async () => {
  azure.metadata.resetstate = state;
  azure.cli = async () => { throw new b.Failure('ERROR: {"code":"DeploymentFailed"}'); };
  await assert.rejects(() => b.provision(azure, manifest, 0, output, false, {}), b.DeploymentFailed);
  assert.deepEqual(azure.registry(), {}); assert.equal(azure.metadata.resetstate, state); assert.equal(azure.metadata.resetgeneration, generation);
});
test('unknown deployment outcome marks member uncertain without clearing fence', async () => {
  azure.metadata.resetstate = FENCED; azure.cli = async () => { throw new b.Failure('connection reset'); };
  await failure(() => b.provision(azure, manifest, 0, output, false, {}));
  assert.deepEqual(Object.values(azure.registry()).map((entry: any) => entry.state), ['uncertain']);
  assert.equal(azure.metadata.resetstate, FENCED);
});
for (const state of ['ready', 'fenced', CLEANING]) test(`public mutation entrypoints reject incompatible authority ${state}`, async () => {
  azure.metadata.resetstate = state; const before = { ...azure.metadata };
  for (const entry of [
    () => b.provision(azure, manifest, 0, output, false, {}),
    () => b.reconcile_command(azure, manifest, ''),
    () => b.stop_cluster(azure), () => b.occupancy(azure, 'occupy', b.uuid()),
  ]) await failure(entry);
  assert.deepEqual(azure.metadata, before); assert.deepEqual(azure.events, []);
});
test('nonempty fenced authority refuses provision', async () => {
  azure.metadata.resetstate = FENCED; azure.register(undefined, 'uncertain');
  await failure(() => b.provision(azure, manifest, 0, output, false, {})); assert.deepEqual(azure.events, []);
});
test('stop hold first blocks bootstrap before hosted I/O and bootstrap first blocks stop', async () => {
  azure.metadata.resetstate = FENCED; azure.metadata.occupancy = 'stopping'; azure.metadata.occupancyid = b.uuid();
  await failure(() => b.provision(azure, manifest, 0, output, false, {})); assert.deepEqual(azure.events, []);
  azure.metadata.occupancy = 'released'; const member = b.uuid();
  await new PublisherControl(azure).admit(member, generation, true);
  const before = azure.events.length; await failure(() => b.stop_cluster(azure)); assert.equal(azure.events.length, before);
});
test('stop between child drain and parent admission prevents parent KQL', async () => {
  azure.metadata.resetstate = FENCED; const { child } = coordinateProvision();
  child.then = async () => { azure.metadata.occupancy = 'stopping'; azure.metadata.occupancyid = b.uuid(); };
  await failure(() => b.provision(azure, manifest, 0, output, false, {}));
  assert.ok(!azure.events.some(event => event[0] === 'kql'));
});
test('occupy wins before ready provision/released reconcile custody: both fail before I/O', async () => {
  await b.occupancy(azure, 'occupy', b.uuid()); const before = azure.events.length;
  await failure(() => b.provision(azure, manifest, 0, output, false, {}));
  await failure(() => b.reconcile_command(azure, manifest, '')); assert.equal(azure.events.length, before);
});
test('ready provision custody wins before occupy: occupant blocked until ready again', async () => {
  initialize(); coordinateProvision();
  const original = azure.cli.bind(azure);
  azure.cli = async (...args) => {
    if (args[0] === 'deployment') await failure(() => b.occupancy(azure, 'occupy', b.uuid()));
    return original(...args);
  };
  await b.provision(azure, manifest, 0, output, false, {});
  await b.occupancy(azure, 'occupy', b.uuid()); assert.equal(azure.metadata.occupancy, 'testing');
});
test('released reconcile custody excludes occupy throughout KQL then releases atomically', async () => {
  initialize(); const original = azure.kql.bind(azure);
  azure.kql = async command => {
    assert.equal(azure.metadata.resetstate, FENCED); assert.equal(Object.keys(azure.registry()).length, 1);
    await failure(() => b.occupancy(azure, 'occupy', b.uuid())); return original(command);
  };
  await b.reconcile_command(azure, manifest, '');
  assert.equal(azure.metadata.resetstate, READY); assert.deepEqual(azure.registry(), {});
});
test('occupied reconcile pins owner/generation inside admission; member blocks release', async () => {
  initialize(); const owner = b.uuid(); await b.occupancy(azure, 'occupy', owner);
  const original = azure.kql.bind(azure);
  azure.kql = async command => {
    assert.equal(Object.keys(azure.registry()).length, 1);
    await failure(() => b.occupancy(azure, 'release', owner, undefined, generation)); return original(command);
  };
  await b.reconcile_command(azure, manifest, owner, 0, generation);
  assert.deepEqual(azure.registry(), {}); assert.equal(azure.metadata.occupancy, 'testing');
});
test('owner changes after public read but before admission: fails before ARM/KQL', async () => {
  initialize(); const owner = b.uuid(); await b.occupancy(azure, 'occupy', owner);
  const original = azure.head_control.bind(azure); let reads = 0;
  azure.head_control = async () => { if (++reads === 2) azure.metadata.occupancyid = b.uuid(); return original(); };
  const before = azure.events.length;
  await failure(() => b.reconcile_command(azure, manifest, owner, 0, generation)); assert.equal(azure.events.length, before);
});
test('test reconcile requires captured generation and forwards readiness wait', async () => {
  const owner = b.uuid(); await b.occupancy(azure, 'occupy', owner);
  await failure(() => b.reconcile_command(azure, manifest, owner));
  await failure(() => b.reconcile_command(azure, manifest, owner, 0, b.uuid()));
  mock.method(b.runtime, 'sleep', async (seconds: number) => { elapsed += seconds; initialize(); });
  await b.reconcile_command(azure, manifest, owner, 10, generation); assert.equal(elapsed, 1);
});
test('unknown cluster start keeps uncertain member and blocks stop', async () => {
  initialize(); azure.cluster_state = 'Stopped';
  azure.cli = async () => { throw new b.Failure('unknown start'); };
  await failure(() => b.reconcile_command(azure, manifest, ''));
  assert.deepEqual(Object.values(azure.registry()).map((entry: any) => entry.state), ['uncertain']);
  await failure(() => b.stop_cluster(azure));
});
test('NotReady and definite deployment errors clean-release while unknown held work remains blocking', async () => {
  await assert.rejects(() => b.reconcile_command(azure, manifest, ''), b.NotReady);
  assert.deepEqual(azure.registry(), {}); assert.equal(azure.metadata.resetstate, READY);
  const owner = b.uuid(); await b.occupancy(azure, 'occupy', owner);
  await assert.rejects(() => b.reconcile_command(azure, manifest, owner, 0, generation), b.NotReady);
  assert.deepEqual(azure.registry(), {});
});
for (const raw of [undefined, '{ }', '[]', 'null', 'bad', '{"x":{}}',
  '{"00000000-0000-0000-0000-000000000001":{},"00000000-0000-0000-0000-000000000001":{}}',
  '{' + ' '.repeat(4096) + '}'])
  test(`registry wire rejects missing/malformed/noncanonical/duplicate/oversized ${String(raw).slice(0, 100)}`, () => {
    const metadata = { ...azure.metadata };
    if (raw === undefined) delete metadata.publishers; else metadata.publishers = raw;
    assert.throws(() => b.publisher_registry(metadata), b.Failure);
  });
test('unknown state/extra entry fields/invalid IDs, noncanonical field order all fail closed', () => {
  const publisher = b.uuid();
  for (const entry of [{ generation, state: 'gone' }, { generation, state: 'active', lease: 1 },
    { generation: 'not-uuid', state: 'active' }])
    assert.throws(() => b.publisher_registry({ ...azure.metadata, publishers: b.canonical_json({ [publisher]: entry }) }), b.Failure);
  const unsorted = `{"${publisher}":{"state":"active","generation":"${generation}"}}`;
  assert.throws(() => b.publisher_registry({ ...azure.metadata, publishers: unsorted }), b.Failure);
  for (const state of ['active', 'uncertain']) {
    azure.metadata.publishers = b.canonical_json({ [publisher]: { generation, state } });
    assert.throws(() => b.require_drained_publishers(azure.metadata), b.Failure);
  }
  azure.metadata.publishers = '{}'; b.require_drained_publishers(azure.metadata);
});
test('entrypoint config writes private file and CLI validation never falls through to Azure', async () => {
  const env = path.join(directory, '.env'); fs.writeFileSync(env, `AZURE_SUBSCRIPTION_ID=${SUB}\n`);
  assert.equal(await b.main(['config', '--env-file', env, '--output', output]), 0);
  assert.equal(fs.statSync(output).mode & 0o777, 0o600);
  for (const args of [['config', '--wait-seconds', '601'], ['config', '--wait-seconds', 'NaN'], ['unknown']])
    assert.equal(await b.main([...args, '--env-file', env]), 1);
  assert.equal(await b.main(['--help']), 0);
});
test('main status reports reset database absence without querying Kusto', async () => {
  const env = path.join(directory, '.env'); fs.writeFileSync(env, `AZURE_SUBSCRIPTION_ID=${SUB}\n`);
  azure.databases = [];
  for (const name of ['arm', 'collection', 'blobs', 'paths', 'head_control', 'set_control', 'kql', 'cli'] as const)
    mock.method(b.Azure.prototype, name, azure[name].bind(azure));
  assert.equal(await b.main(['status', '--env-file', env]), 0);
  assert.ok(!azure.events.some(event => event[0] === 'kql'));
});
test('main dispatches versioned mutation paths but cleanup-owned authority blocks every entrypoint', async () => {
  const env = path.join(directory, '.env'); fs.writeFileSync(env, `AZURE_SUBSCRIPTION_ID=${SUB}\n`);
  const owner = b.uuid();
  for (const name of ['arm', 'collection', 'blobs', 'paths', 'head_control', 'set_control', 'kql', 'cli'] as const)
    mock.method(b.Azure.prototype, name, azure[name].bind(azure));
  assert.equal(await b.main(['occupy', '--env-file', env, '--occupancy-id', owner, '--output', output]), 0);
  assert.equal(b.load_env(output).ARROWCOL_RESET_GENERATION, generation);
  assert.equal(await b.main(['release', '--env-file', env, '--occupancy-id', owner, '--reset-generation', generation]), 0);
  azure.metadata.resetstate = CLEANING;
  azure.events = [];
  for (const command of ['reconcile', 'occupy', 'release', 'stop']) {
    const args = [command, '--env-file', env, '--tables', manifest, '--occupancy-id', owner, '--reset-generation', generation];
    if (command === 'stop') args.push('--execute');
    assert.equal(await b.main(args), 1, command);
  }
  assert.deepEqual(azure.events, []);
});
for (const xml of [
  '<EnumerationResults><Blobs><Blob></Blobs></EnumerationResults>',
  '<EnumerationResults><Blobs><Blob><Name>x&unknown;</Name></Blob></Blobs></EnumerationResults>',
  '<EnumerationResults><Blobs><Blob><Name>x</Name></Blob></Blobs>',
  '<EnumerationResults/><foreign/>', '<!DOCTYPE x SYSTEM "file:///private"><EnumerationResults/>',
]) test('malformed inventory XML cannot be mistaken for physical emptiness: ' + xml.slice(0, 100), async () => {
  const real = new b.Azure(azure.c);
  real.blob = async () => [Buffer.from(xml), {}];
  await failure(() => real.blobs(azure.c.data));
});
test('inventory parser supports XML declaration, comments, CDATA and numeric entities without expansion', () => {
  const root = b.parse_xml('<?xml version="1.0"?><!--safe--><Root attr="&amp;"><Name><![CDATA[a&amp;b]]>&#x2f;&#47;</Name></Root>');
  assert.equal(root.children[0].text, 'a&amp;b//');
});
test('failed file fsync cleans unpublished intermediates and never claims cleanup', () => {
  mock.method(fs, 'fsyncSync', (_fd: number) => { throw new Error('fsync failure'); });
  assert.throws(() => b.write_reset_record(recordPath, makeRecord()), /fsync failure/);
  assert.deepEqual(fs.readdirSync(directory), ['tables.json']); assert.deepEqual(azure.events, []);
});
