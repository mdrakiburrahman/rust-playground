import test from 'node:test';
import assert from 'node:assert/strict';
import { HttpFailure } from '../../../infra/azure/bootstrap.ts';
import { bodyReads, LATE_MARGIN_MS, logsQuery, parseReads, rawQuery, readsQuery, transientLogFailure, waitForLogs,
  type ReadScope } from './storage-reads.js';

const PRINCIPAL = '00000000-0000-0000-0000-000000000001';
const SCOPE: ReadScope = { workspace: 'ws', account: 'acct', container: 'experimentqap', requester: PRINCIPAL,
  prefix: 'runs/r1/optimize/' };
const WINDOW = { from: Date.UTC(2025, 0, 1, 10, 0), mutationAt: Date.UTC(2025, 0, 1, 10, 2), windowEnd: Date.UTC(2025, 0, 1, 10, 5) };
const BLOB = 'https://acct.blob.core.windows.net/experimentqap/runs/r1/optimize/';
const COLUMNS = ['Phase', 'Path', 'OperationName', 'StatusCode', 'Requests', 'Bytes', 'Last'].map(name => ({ name, type: 'string' }));

const response = (rows: unknown[][]) => ({ tables: [{ name: 'PrimaryResult', columns: COLUMNS, rows }] });
const row = (phase: string, path: string, operation = 'GetBlob', bytes = 100, last = '2025-01-01T10:01:00Z', status: unknown = '200') =>
  [phase, path.startsWith('https://') ? path : BLOB + path, operation, status, 1, bytes, last];

test('readsQuery is a copy-pasteable KQL scoped to the trial prefix and the Kusto identity', () => {
  const text = readsQuery(SCOPE, WINDOW);
  assert.match(text, /^StorageBlobLogs\n/);
  assert.match(text, /TimeGenerated >= datetime\(2025-01-01T10:00:00\.000Z\)/);
  assert.match(text, /RequesterObjectId =~ "00000000-0000-0000-0000-000000000001"/);
  assert.match(text, /Uri contains "\/experimentqap\/runs\/r1\/optimize\/"/);
  assert.match(text, /TimeGenerated < datetime\(2025-01-01T10:02:00\.000Z\), "warm-up",/);
  assert.match(text, /TimeGenerated < datetime\(2025-01-01T10:05:00\.000Z\), "after-mutation", "late"/);
  assert.match(rawQuery(SCOPE, WINDOW.from), /\| project TimeGenerated, OperationName, StatusCode, ResponseBodySize, Uri/);
  for (const bad of [{ prefix: 'runs/r1/' }, { prefix: 'runs/r1/../x/' }, { requester: 'x" or 1==1' }, { account: 'Acct' }])
    assert.throws(() => readsQuery({ ...SCOPE, ...bad }, WINDOW), /Invalid storage read scope/);
});

test('parseReads keeps per-file requests under the prefix and decodes paths', () => {
  const groups = parseReads(response([
    row('warm-up', 'ingestion%3D2025/part-0.parquet'),
    row('after-mutation', 'https://acct.dfs.core.windows.net/experimentqap/runs/r1/optimize/_delta_log/00000000000000000001.json',
      'ReadFile', 50, '2025-01-01T10:03:00Z', 200),
    row('warm-up', 'https://acct.dfs.core.windows.net/experimentqap?resource=filesystem&directory=runs/r1/optimize', 'ListFilesystemDir'),
    row('warm-up', 'https://other.blob.core.windows.net/experimentqap/runs/r1/optimize/x.parquet'),
    row('warm-up', 'https://acct.blob.core.windows.net/experimentqap/runs/r1/optimize_2/x.parquet'),
  ]), SCOPE);
  assert.deepEqual(groups, [
    { phase: 'warm-up', path: 'ingestion=2025/part-0.parquet', operation: 'GetBlob', status: 200, requests: 1, bytes: 100,
      last: '2025-01-01T10:01:00Z' },
    { phase: 'after-mutation', path: '_delta_log/00000000000000000001.json', operation: 'ReadFile', status: 200, requests: 1, bytes: 50,
      last: '2025-01-01T10:03:00Z' },
  ]);
  assert.throws(() => parseReads({ tables: [] }, SCOPE), /failed, partial or ambiguous/);
  assert.throws(() => parseReads({ ...response([]), partialError: { code: 'x' } }, SCOPE), /failed, partial or ambiguous/);
  assert.throws(() => parseReads(response([row('soon', 'a.parquet')]), SCOPE), /malformed/);
});

test('bodyReads counts distinct files actually read, optionally restricted to a path set', () => {
  const groups = parseReads(response([
    row('after-mutation', 'p=a/1.parquet'), row('after-mutation', 'p=a/1.parquet', 'ReadFile', 30),
    row('after-mutation', 'p=b/2.parquet', 'GetBlobProperties', 0), row('after-mutation', 'p=c/3.parquet', 'GetBlob', 100, undefined, 404),
    row('after-mutation', 'p=d/4.parquet', 'GetBlob', 0), row('warm-up', 'p=e/5.parquet'),
    row('warm-up', '_delta_log/00000000000000000000.json'),
  ]), SCOPE);
  assert.deepEqual(bodyReads(groups, 'after-mutation'), { files: 1, bytes: 130, requests: 2, paths: ['p=a/1.parquet'] });
  assert.equal(bodyReads(groups, 'after-mutation', new Set(['p=e/5.parquet'])).files, 0);
  assert.equal(bodyReads(groups, 'warm-up').files, 1, 'Delta log reads are not data files');
});

function clock(start: number) {
  let now = start;
  return { now: () => now, sleep: async (ms: number) => { now += ms; } };
}

test('waitForLogs completes once late traffic arrived and the window stopped changing', async () => {
  const time = clock(WINDOW.windowEnd);
  const lateAt = new Date(WINDOW.windowEnd + LATE_MARGIN_MS + 1000).toISOString();
  const answers = [
    response([row('warm-up', 'a.parquet')]),
    response([row('warm-up', 'a.parquet'), row('after-mutation', 'b.parquet')]),
    response([row('warm-up', 'a.parquet'), row('after-mutation', 'b.parquet'), row('late', '_delta_log/1.json', 'GetBlob', 9, lateAt)]),
    response([row('warm-up', 'a.parquet'), row('after-mutation', 'b.parquet'), row('late', '_delta_log/1.json', 'GetBlob', 9, lateAt)]),
  ];
  const queries: string[] = [];
  const statuses: string[] = [];
  let nudges = 0;
  const result = await waitForLogs({ query: async query => { queries.push(query); return answers.shift(); }, scope: SCOPE,
    window: WINDOW, deadline: WINDOW.windowEnd + 600_000, pollMs: 30_000, signal: new AbortController().signal, ...time,
    nudge: async () => { nudges++; }, onPoll: (_, status) => statuses.push(status) });
  assert.equal(result.status, 'complete');
  assert.equal(result.polls, 4);
  assert.equal(result.groups.length, 3);
  assert.equal(nudges, 1);
  assert.equal(queries[0], readsQuery(SCOPE, WINDOW));
  assert.deepEqual(statuses, ['waiting for logs past the window', 'waiting for logs past the window',
    'caught up, confirming nothing else arrives', 'complete']);
});

test('waitForLogs returns pending at its deadline and retries only transient failures', async () => {
  const time = clock(WINDOW.windowEnd);
  let calls = 0;
  const pending = await waitForLogs({ query: async () => {
    if (calls++ === 0) throw new HttpFailure(429, 'throttled');
    return response([row('warm-up', 'a.parquet')]);
  }, scope: SCOPE, window: WINDOW, deadline: WINDOW.windowEnd + 100_000, pollMs: 30_000, signal: new AbortController().signal, ...time });
  assert.equal(pending.status, 'pending');
  assert.equal(pending.polls, 4);
  assert.equal(pending.groups.length, 1);
  assert.equal(pending.lastError, undefined);

  await assert.rejects(waitForLogs({ query: async () => { throw new HttpFailure(403, 'no reader role'); }, scope: SCOPE, window: WINDOW,
    deadline: WINDOW.windowEnd + 100_000, pollMs: 30_000, signal: new AbortController().signal, ...clock(0) }), /no reader role/);
  assert.equal(transientLogFailure(new HttpFailure(400, "'StorageBlobLogs': Failed to resolve table")), true);
  assert.equal(transientLogFailure(new HttpFailure(400, 'syntax error')), false);
  assert.equal(transientLogFailure(new HttpFailure(500, 'oops')), true);
});

test('logsQuery posts KQL to the workspace query API', async () => {
  const seen: unknown[] = [];
  const azure = { request: async (...args: unknown[]) => { seen.push(args); return [Buffer.from('{"tables":[]}'), {}]; } };
  const query = logsQuery(azure as never, PRINCIPAL);
  assert.deepEqual(await query('StorageBlobLogs | take 1', new AbortController().signal), { tables: [] });
  assert.deepEqual((seen[0] as unknown[]).slice(0, 4), ['POST', `https://api.loganalytics.azure.com/v1/workspaces/${PRINCIPAL}/query`,
    'https://api.loganalytics.io', '{"query":"StorageBlobLogs | take 1"}']);
  assert.throws(() => logsQuery(azure as never, 'not-a-guid'), /customer ID/);
});
