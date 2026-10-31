import test from 'node:test';
import assert from 'node:assert/strict';
import { HttpFailure } from '../../../infra/azure/bootstrap.ts';
import { compareGroups, describeSample, identifier, Kusto, kql, parseExtents, parseSample, ParityFailure, ready, retainedExtentRows,
  tables, transientKusto, unacceleratedRows, UNACCELERATED_EXTENT, type KustoClient } from './kusto.js';

const SOURCE = 'abfss://experimentqap@acct.dfs.core.windows.net/runs/r1/optimize;managed_identity=00000000-0000-0000-0000-000000000001';
const E1 = '11111111-1111-1111-1111-111111111111';
const E2 = '22222222-2222-2222-2222-222222222222';
const E3 = '33333333-3333-3333-3333-333333333333';

const v1 = (columns: string[], rows: unknown[][]) =>
  ({ Tables: [{ TableName: 'Table_0', Columns: columns.map(ColumnName => ({ ColumnName })), Rows: rows }] });
const v2 = (columns: string[], rows: unknown[][], status = 0) => [
  { FrameType: 'DataSetHeader' },
  { FrameType: 'DataTable', TableKind: 'PrimaryResult', Columns: columns.map(ColumnName => ({ ColumnName })), Rows: rows },
  { FrameType: 'DataTable', TableKind: 'QueryCompletionInformation', Columns: [{ ColumnName: 'StatusCode' }], Rows: [[status]] },
  { FrameType: 'DataSetCompletion', HasErrors: false },
];

const details = (state: Record<string, unknown>) => v1(['TableName', 'QueryAccelerationState'], [['t', JSON.stringify(state)]]);
const statistics = (percent: number, pending: number) =>
  v1(['AccelerationCompletePercentage', 'AccelerationPendingDataFilesCount', 'AccelerationPendingDataFilesSize'], [[percent, pending, 0]]);

test('printed KQL is the exact, quoted command set the demo runs', () => {
  assert.equal(kql.create('qap_r1_optimize', SOURCE),
    `.create-or-alter external table ['qap_r1_optimize'] kind=delta (h@'${SOURCE}')`);
  assert.match(kql.policy('qap_r1_optimize', 'principal'),
    /^\.alter external table \['qap_r1_optimize'\] policy query_acceleration '\{"IsEnabled":true,"Hot":"7\.00:00:00",.*"ManagedIdentity":"principal"\}'$/);
  assert.equal(kql.drop('t'), `.drop external table ['t'] ifexists`);
  assert.equal(kql.details('t'), `.show external table ['t'] details`);
  assert.match(kql.extents('t'), /^external_table\("t"\)\n\| summarize Rows=count\(\) by Partition=__arrowcol_ingestion, Extent=tostring\(extent_id\(\)\)/);
  assert.match(kql.parity('t', 4), /\| take 5\n/);
  assert.throws(() => identifier("t'];.drop table x"), /Invalid Kusto table/);
  assert.throws(() => kql.extents('a"b'), /Invalid Kusto table/);
});

test('tables reads v1 and v2 responses and rejects error frames', () => {
  assert.deepEqual(tables(v1(['A'], [[1]])), [{ kind: 'PrimaryResult', columns: ['A'], rows: [[1]] }]);
  assert.equal(tables(v2(['A'], [[1]])).length, 2);
  assert.throws(() => tables(v2(['A'], [[1]], 1)), /error status/);
  assert.throws(() => tables([{ FrameType: 'DataSetCompletion', HasErrors: true }]), /partial, cancelled or failed/);
  assert.throws(() => tables({ Exceptions: ['boom'], Tables: [] }), /Kusto returned an error/);
  assert.throws(() => tables(v1(['A', 'B'], [[1]])), /Malformed Kusto result row/);
});

test('a QAP sample is ready only on the exact Delta version with every row accelerated', () => {
  const sample = parseSample(details({ TableVersion: 3, CompletionPercentage: 100, AcceleratedRowCount: 96, IsHealthy: true,
    LastIngestionDateTime: '2025-01-01T00:00:00Z' }), statistics(100, 0), 'now');
  assert.deepEqual(sample, { at: 'now', version: 3, completionPercent: 100, pendingFiles: 0, pendingBytes: 0, acceleratedRows: 96,
    healthy: true, lastIngestionAt: '2025-01-01T00:00:00Z' });
  assert.equal(ready(sample, { version: 3, rows: 96 }), true);
  assert.equal(ready(sample, { version: 4, rows: 96 }), false);
  assert.equal(ready({ ...sample, pendingFiles: 1 }, { version: 3, rows: 96 }), false);
  assert.equal(ready({ ...sample, acceleratedRows: 80 }, { version: 3, rows: 96 }), false);
  assert.equal(describeSample({ at: 'now', version: 2, completionPercent: 50, pendingFiles: 3, healthy: false }, { version: 3, rows: 96 }),
    'Delta v2/3, 50% accelerated, 3 files pending, ?/96 rows, UNHEALTHY');
  assert.equal(describeSample({ at: 'now', version: -1, completionPercent: -1, pendingFiles: -1, acceleratedRows: -1, healthy: true },
    { version: 2, rows: 96 }), 'QAP has not started yet (TableVersion -1); target Delta v2, 96 rows');
  assert.match(describeSample({ ...sample, pendingFiles: -1 }, { version: 3, rows: 96 }), /, \? files pending,/);
  assert.throws(() => parseSample(v1(['QueryAccelerationState'], []), statistics(0, 0), 'now'), /missing or duplicate/);
});

test('extent inventory counts rows that stayed in their original extents', () => {
  const before = parseExtents([['p=a', E1, 10], ['p=b', E2, 5], ['p=c', UNACCELERATED_EXTENT, 3], ['p=d', E3, 7]]);
  const after = parseExtents([['p=a', E1.toUpperCase(), 10], ['p=b', E3, 5], ['p=c', UNACCELERATED_EXTENT, 3], ['p=e', E3, 1]]);
  assert.deepEqual(retainedExtentRows(before, after, new Set(['p=a', 'p=b', 'p=c'])), { before: 18, kept: 10 });
  assert.equal(unacceleratedRows(after), 3);
  assert.throws(() => parseExtents([['p=a', E1, 1], ['p=a', E1, 2]]), /malformed or duplicate/);
  assert.throws(() => parseExtents([['p=a', 'not-a-guid', 1]]), /malformed or duplicate/);
});

test('compareGroups requires exact multiplicities for every oracle group', () => {
  const expected = [{ partition: 'p=a', id: 1, value: 2, payloadSha256: 'ab', multiplicity: 2 },
    { partition: 'p=b', id: 3, value: 4, payloadSha256: 'cd', multiplicity: 1 }];
  compareGroups(expected, [['p=a', 1, 2, 'AB', 2], ['p=b', 3, 4, 'cd', 1]]);
  assert.throws(() => compareGroups(expected, [['p=a', 1, 2, 'ab', 1], ['p=b', 3, 4, 'cd', 1]]), /multiplicity 1; the oracle expects 2/);
  assert.throws(() => compareGroups(expected, [['p=a', 1, 2, 'ab', 2]]), /returned 1 row groups/);
  assert.throws(() => compareGroups(expected, [['p=a', 1, 2, 'ab', 2], ['p=a', 1, 2, 'ab', 2]]), /oracle expects 0|omitted/);
});

function fakeKusto(respond: (path: string, body: { db: string; csl: string }) => unknown) {
  const calls: { path: string; body: { db: string; csl: string; properties?: string }; headers: Record<string, string> }[] = [];
  const azure = {
    c: { uri: 'https://cluster.kusto.windows.net' },
    request: async (method: string, url: string, resource: string, body?: string | Buffer | null, headers: Record<string, string> = {}) => {
      assert.equal(method, 'POST');
      assert.equal(resource, 'https://kusto.kusto.windows.net');
      const path = url.slice('https://cluster.kusto.windows.net'.length);
      const parsed = JSON.parse(String(body));
      calls.push({ path, body: parsed, headers });
      const value = respond(path, parsed);
      if (value instanceof Error) throw value;
      return [Buffer.from(JSON.stringify(value)), {}] as [Buffer | null, Record<string, string>];
    },
  };
  const sleeps: number[] = [];
  const kusto = new Kusto(azure as unknown as KustoClient, 'experimentqap', 'r1', async ms => { sleeps.push(ms); });
  return { kusto, calls, sleeps };
}

const signal = new AbortController().signal;

test('Kusto client retries transient failures and tags requests with the run', async () => {
  let failures = 1;
  const { kusto, calls, sleeps } = fakeKusto(() => failures-- > 0 ? new HttpFailure(503, 'busy') : v1(['A'], [[1]]));
  await kusto.drop('qap_r1_optimize', signal);
  assert.equal(calls.length, 2);
  assert.deepEqual(sleeps, [2000]);
  assert.deepEqual(calls[1]!.body, { db: 'experimentqap', csl: `.drop external table ['qap_r1_optimize'] ifexists` });
  assert.match(calls[1]!.headers['x-ms-client-request-id']!, /^ArrowColQapDemo;r1;/);

  const denied = fakeKusto(() => new HttpFailure(400, 'bad'));
  await assert.rejects(denied.kusto.drop('t', signal), /bad/);
  assert.equal(denied.calls.length, 1);
  assert.equal(transientKusto(new HttpFailure(429, 'slow')), true);
  assert.equal(transientKusto(new Error('plain')), false);
});

test('create waits out 403s while the managed identity grant propagates', async () => {
  let denials = 2;
  const { kusto, calls, sleeps } = fakeKusto(() => denials-- > 0 ? new HttpFailure(403, 'forbidden') : v1(['A'], [[1]]));
  await kusto.create('t', SOURCE, 'principal', signal, Date.now() + 60_000);
  assert.deepEqual(calls.map(call => call.body.csl.split(' ')[0]), ['.create-or-alter', '.create-or-alter', '.create-or-alter', '.alter']);
  assert.deepEqual(sleeps, [5000, 5000]);
  const late = fakeKusto(() => new HttpFailure(403, 'forbidden'));
  await assert.rejects(late.kusto.create('t', SOURCE, 'principal', signal, Date.now() + 1000), /forbidden/);
});

test('sample, extents, parity and marker use management and v2 queries', async () => {
  const { kusto, calls } = fakeKusto((path, body) => {
    if (body.csl.endsWith('details')) return details({ TableVersion: 1, CompletionPercentage: 100, AcceleratedRowCount: 2, IsHealthy: true });
    if (body.csl.endsWith('statistics')) return statistics(100, 0);
    assert.equal(path, '/v2/rest/query');
    if (body.csl.includes('extent_id')) return v2(['Partition', 'Extent', 'Rows'], [['p=a', E1, 2]]);
    if (body.csl.includes('multiplicity')) return v2(['partition', 'id', 'value', 'payloadSha256', 'multiplicity'], [['p=a', 1, 1, 'ab', 2]]);
    return v2(['Count'], [[2]]);
  });
  assert.equal(ready(await kusto.sample('t', signal), { version: 1, rows: 2 }), true);
  assert.deepEqual(await kusto.extents('t', signal), [{ partition: 'p=a', extent: E1, rows: 2 }]);
  await kusto.parity('t', [{ partition: 'p=a', id: 1, value: 1, payloadSha256: 'ab', multiplicity: 2 }], signal);
  await assert.rejects(kusto.parity('t', [{ partition: 'p=a', id: 1, value: 1, payloadSha256: 'ab', multiplicity: 3 }], signal),
    error => error instanceof ParityFailure && /^t: /.test(error.message));
  assert.equal(await kusto.marker('t', signal), 2);
  const query = calls.find(call => call.path === '/v2/rest/query')!;
  assert.deepEqual(JSON.parse(query.body.properties!), { Options: { query_results_cache_max_age: '00:00:00' } });
});
