import assert from 'node:assert/strict';
import { mock, test } from 'node:test';
import { Azure, Config, HttpFailure } from '../../../infra/azure/bootstrap.ts';
import { durationMs, identifier, Kusto, object, parseQuery, parseSample, ready, repointRequiresPolicyRemoval, tables, waitForRolePropagation } from './kusto.js';

function response(columns: string[], rows: unknown[][]): unknown {
  return { Tables: [{ Columns: columns.map(ColumnName => ({ ColumnName })), Rows: rows }] };
}

test('public QAP dynamic state accepts JSON strings and preserves absent measurements', () => {
  const sample = parseSample(response(['QueryAccelerationState'], [[JSON.stringify({
    TableVersion: 3, IsHealthy: true, AcceleratedRowCount: 20,
  })]]), response(['AccelerationCompletePercentage', 'AccelerationPendingDataFilesCount'], [[100, 0]]), 'now');
  assert.equal(sample.version, 3);
  assert.equal(sample.pendingBytes, undefined);
  assert.equal(ready(sample, { version: 3, rows: 20, files: [], fingerprint: 'x' }), true);
  assert.equal(ready(sample, { version: 4, rows: 20, files: [], fingerprint: 'x' }), false);
});
test('missing exact-version or health diagnostics do not imply readiness', () => {
  const sample = parseSample(response(['QueryAccelerationState'], [[{}]]),
    response(['AccelerationCompletePercentage'], [[100]]), 'now');
  assert.equal(ready(sample, { version: 0, rows: 0, files: [], fingerprint: 'x' }), false);
  assert.equal(sample.pendingFiles, undefined);
});
test('failed and partial Kusto responses cannot become successful empty results', () => {
  assert.throws(() => tables({ error: { code: 'Denied' } }), /error/);
  assert.throws(() => tables([{ FrameType: 'DataSetCompletion', HasErrors: true }]), /partial/);
  assert.throws(() => tables(response(['id'], [[1, 2]])), /Malformed/);
  assert.throws(() => tables([{ FrameType: 'DataTable', TableKind: 'QueryCompletionInformation',
    Columns: [{ ColumnName: 'StatusCode' }], Rows: [[-1]] }]), /completion/);
});
test('query CPU is query-scoped; missing fallback downloads are not zero', () => {
  const value = [
    { FrameType: 'DataTable', TableKind: 'PrimaryResult', Columns: [{ ColumnName: 'id' }], Rows: [[1]] },
    { FrameType: 'DataTable', TableKind: 'QueryCompletionInformation',
      Columns: ['StatusCode', 'EventTypeName', 'Payload'].map(ColumnName => ({ ColumnName })),
      Rows: [[0, 'QueryResourceConsumption', JSON.stringify({
        resource_usage: { cpu: { 'total cpu': '00:00:00.025' } },
        input_dataset_statistics: { extents: { scanned: 1 } },
      })]] },
    { FrameType: 'DataSetCompletion', HasErrors: false },
  ];
  const result = parseQuery(value, 'request-id');
  assert.equal(result.queryCpuMs, 25);
  assert.equal(result.scannedExtents, 1);
  assert.equal(result.remoteDownloadedItems, undefined);
  assert.equal(durationMs('1.01:02:03.5'), 90_123_500);
  assert.equal(durationMs(undefined), undefined);
});
test('identifiers are allowlisted before interpolation', () => {
  assert.equal(identifier('fixture'), "['fixture']");
  assert.throws(() => identifier("fixture']; .drop database x"), /Invalid/);
});
test('new-resource role propagation retries only explicit forbidden responses and records waiting', async () => {
  let attempts = 0;
  const events: string[] = [];
  await waitForRolePropagation(async () => {
    if (++attempts === 1) throw new HttpFailure(403, 'role not propagated');
  }, new AbortController().signal, kind => events.push(kind), { timeoutMs: 1000, intervalMs: 1 });
  assert.equal(attempts, 2);
  assert.deepEqual(events, ['role-propagation-wait']);
  const error = new HttpFailure(400, 'invalid command');
  await assert.rejects(waitForRolePropagation(async () => { throw error; },
    new AbortController().signal, () => {}, { timeoutMs: 1000, intervalMs: 1 }), value => value === error);
});
test('role propagation deadline and cancellation remain failures', async () => {
  const denied = new HttpFailure(403, 'permanent denial');
  await assert.rejects(waitForRolePropagation(async () => { throw denied; },
    new AbortController().signal, () => {}, { timeoutMs: 0, intervalMs: 1 }), value => value === denied);
  const controller = new AbortController();
  await assert.rejects(waitForRolePropagation(async () => { throw denied; }, controller.signal,
    () => controller.abort(new Error('cancelled')), { timeoutMs: 1000, intervalMs: 1 }), /aborted/i);
});
test('only the specific native enabled-QAP connection-string guard is classified as a blocked repoint', () => {
  const message = 'Cannot alter the connection string of external table \'source_generation\' as it has query_acceleration policy enabled. ' +
    'Please remove the query acceleration policy before altering the connection string.';
  const body = JSON.stringify({ error: { '@message': message } });
  assert.equal(repointRequiresPolicyRemoval(new HttpFailure(400, 'HTTP 400: ' + body)), true);
  assert.equal(repointRequiresPolicyRemoval(new HttpFailure(400, 'HTTP 400: ' + JSON.stringify({
    error: { '@message': 'Invalid KQL syntax.' },
  }))), false);
  assert.equal(repointRequiresPolicyRemoval(new HttpFailure(403, 'HTTP 400: ' + body)), false);
  assert.equal(repointRequiresPolicyRemoval(new Error('HTTP 400: ' + body)), false);
  assert.equal(repointRequiresPolicyRemoval(new HttpFailure(400, 'HTTP 400: malformed')), false);
});
test('cluster capability evidence records the deployed public ProductVersion field', async () => {
  const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: '11111111-1111-1111-1111-111111111111' }));
  mock.method(azure, 'arm', async () => ({ properties: { principalId: '22222222-2222-2222-2222-222222222222' } }));
  const events: { kind: string; data: unknown }[] = [];
  const kusto = new Kusto(azure, 'test', (kind, data) => events.push({ kind, data }));
  mock.method(kusto, 'management', async () => response(['ProductVersion'], [['public-engine-version']]));
  await kusto.initialize(new AbortController().signal);
  const capability = events.find(event => event.kind === 'capabilities');
  assert.ok(capability);
  assert.equal(object(capability.data).buildVersion, 'public-engine-version');
});
