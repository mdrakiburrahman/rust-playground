import assert from 'node:assert/strict';
import test from 'node:test';
import { exportedReads, readGroupsResponse, storageExportQuery } from './storage-export.js';
import { bodyReads, parseReads, readsQuery, type ReadScope } from './storage-reads.js';
import { parseStorageReadQuery } from './read-logs.js';

const scope: ReadScope = { workspace: '11111111-1111-4111-8111-111111111111', account: 'arrowcollector1',
  container: 'experimentqap', requester: '22222222-2222-4222-8222-222222222222', prefix: 'runs/run_1/reload/',
  owned: { marker: 'arrowcol-qap/run_1/reload', requestIds: ['33333333-3333-4333-8333-333333333333'] } };
const window = { from: Date.parse('2026-11-07T11:00:00Z'), mutationAt: Date.parse('2026-11-07T11:01:00Z'),
  windowEnd: Date.parse('2026-11-07T11:03:00Z') };
const event = (time: string, correlationId: string, extra: Record<string, unknown> = {}) => ({
  time, category: 'StorageRead', operationName: 'GetBlob', correlationId, statusCode: 206,
  uri: 'https://arrowcollector1.blob.core.windows.net/experimentqap/runs/run_1/reload/p%3D1/data.parquet',
  identity: { type: 'OAuth', requester: { objectId: scope.requester } },
  properties: { accountName: scope.account, responseBodySize: 64, ...extra },
});

test('storage export preserves phase, bytes and raw evidence while excluding all owned and foreign reads', () => {
  const warm = event('2026-11-07T11:00:30Z', 'warm');
  const reload = event('2026-11-07T11:01:30Z', 'reload');
  const late = { ...event('2026-11-07T11:05:00Z', 'late'), uri: warm.uri.replace('data.parquet', '_delta_log/2.json') };
  const ownMarker = event('2026-11-07T11:01:31Z', 'fixture', { userAgentHeader: scope.owned!.marker });
  const ownLostMarker = event('2026-11-07T11:01:32Z', scope.owned!.requestIds[0]!);
  const foreign = { ...reload, identity: { requester: { objectId: '44444444-4444-4444-8444-444444444444' } } };
  const records: Record<string, unknown>[] = [];
  const groups = exportedReads([warm, reload, reload, late, ownMarker, ownLostMarker, foreign].map(row => JSON.stringify(row)),
    scope, window, row => records.push(row));
  assert.equal(records.length, 3);
  assert.equal(records[1]!.RequesterObjectId, scope.requester);
  assert.equal(records[1]!.CorrelationId, 'reload');
  assert.equal(records[1]!.UserAgentHeader, '');
  const parsed = parseReads(readGroupsResponse(groups, scope), scope);
  assert.deepEqual(bodyReads(parsed, 'after-mutation', new Set(['p=1/data.parquet'])),
    { files: 1, bytes: 64, requests: 1, paths: ['p=1/data.parquet'] });
  assert.equal(bodyReads(parsed, 'warm-up').files, 1);
  assert.equal(bodyReads(parsed, 'late').files, 0);
  const kql = readsQuery(scope, window);
  assert.match(kql, /CorrelationId !in/);
  assert.match(kql, /ClientRequestId !in/);
  assert.match(kql, /arrowcol-qap\/run_1\/reload/);
  assert.doesNotMatch(kql, /UserAgentHeader contains "Kusto"/);
  assert.throws(() => exportedReads([JSON.stringify({ ...warm, statusCode: 'invalid' })], scope, window), /malformed/);
});

test('saved storage queries require explicit complete ownership and ordered windows', () => {
  const query = { source: 'storage', logAccount: 'arrowreadlogs1', scope, window };
  assert.deepEqual(parseStorageReadQuery(query), query);
  assert.throws(() => parseStorageReadQuery({ ...query, scope: { ...scope, owned: { marker: scope.owned!.marker, requestIds: [] } } }), /incomplete/);
  assert.throws(() => parseStorageReadQuery({ ...query, window: { ...window, from: window.windowEnd } }), /reversed/);
  assert.throws(() => parseStorageReadQuery({ ...query, source: 'metrics' }), /Invalid diagnostic storage query/);
});

test('storage reader scopes the destination path, caches by ETag and does not silently switch destinations', async () => {
  const prefix = 'resourceId=/subscriptions/sub/resourceGroups/group/providers/Microsoft.Storage/storageAccounts/arrowcollector1/blobServices/default/';
  const name = prefix + 'y=2026/m=11/d=07/h=11/m=00/PT1H.json';
  const listing = `<EnumerationResults><Blobs><Blob><Name>${name}</Name><Properties><Etag>one</Etag><Content-Length>1000</Content-Length></Properties></Blob></Blobs><NextMarker /></EnumerationResults>`;
  let reads = 0;
  const request = async (_method: string, url: string): Promise<[Buffer, Record<string, string>]> => {
    if (url.includes('?')) {
      assert.equal(new URL(url).searchParams.get('prefix'), prefix);
      return [Buffer.from(listing), {}];
    }
    reads++;
    return [Buffer.from(JSON.stringify(event('2026-11-07T11:01:30Z', 'actual')) + '\n'), {}];
  };
  const query = storageExportQuery({ request, c: { subscription: 'sub', group: 'group' } }, 'arrowreadlogs1');
  const context = { scope, window };
  for (let i = 0; i < 2; i++) {
    const response = await query('', new AbortController().signal, context);
    assert.equal(bodyReads(parseReads(response, scope), 'after-mutation').files, 1);
  }
  assert.equal(reads, 1);
  await assert.rejects(query('', new AbortController().signal), /requires complete owned-request receipts/);
});
