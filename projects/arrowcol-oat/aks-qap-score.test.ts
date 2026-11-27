import assert from 'node:assert/strict';
import test from 'node:test';
import { analyzeCoverage } from '../../experiments/azure-qap/src/coverage.js';
import { UNACCELERATED_EXTENT } from '../../experiments/azure-qap/src/kusto.js';
import { parseDeltaVisibility } from './aks-qap-observer.js';
import { buildCoverageInput, coverageObservation, queryProvenance, resourceTrace } from './aks-qap-score.js';

const E1 = '11111111-1111-1111-1111-111111111111', E2 = '22222222-2222-2222-2222-222222222222';
const request = 'ArrowColQapEvidence;fixture;123';
function response(external?: Record<string, number>, firstExtent = E1, nativeTotal = 24) {
  const statistics = { input_dataset_statistics: { rows: { total: nativeTotal },
    extents: { total: firstExtent === UNACCELERATED_EXTENT ? 1 : 2 }, ...(external ? { external_data: external } : {}) },
  resource_usage: { cache: { shards: {} } } };
  return [
    { FrameType: 'DataSetHeader' },
    { FrameType: 'DataTable', TableKind: 'PrimaryResult',
      Columns: ['Rows', 'Extent'].map(ColumnName => ({ ColumnName })), Rows: [[16, firstExtent], [8, E2]] },
    { FrameType: 'DataTable', TableKind: 'QueryCompletionInformation',
      Columns: ['ClientRequestId', 'Level', 'LevelName', 'StatusCode', 'EventTypeName', 'Payload'].map(ColumnName => ({ ColumnName })),
      Rows: [[request, 6, 'Stats', 0, 'QueryResourceConsumption', JSON.stringify(statistics)]] },
    { FrameType: 'DataSetCompletion', HasErrors: false },
  ];
}

test('positive native input and extent provenance prove no fallback without inventing missing counters', () => {
  assert.deepEqual(queryProvenance(response(), request), {
    rows: 24, rowsWithValidExtentId: 24, distinctExtentCount: 2, fallbackDetected: false,
    externalDataRead: false, partial: false, warnings: [],
  });
  assert.throws(() => queryProvenance(response(undefined, E1, 0), request), /Absent external counters/);
  assert.throws(() => queryProvenance(response({ downloaded_items: 0, downloaded_bytes: 0 }), request), /iterated_artifacts/);
  assert.throws(() => queryProvenance(response(), 'another-request'), /bound to the observer/);
});

test('zero-GUID fallback and partial query responses remain explicit rejection evidence', () => {
  const result = queryProvenance(response({ downloaded_items: 1, downloaded_bytes: 4000, iterated_artifacts: 1 },
    UNACCELERATED_EXTENT, 8), request);
  assert.equal(result.fallbackDetected, true);
  assert.equal(result.externalDataRead, true);
  assert.equal(result.rowsWithValidExtentId, 8);
  const frames = response();
  frames[frames.length - 1] = { FrameType: 'DataSetCompletion', HasErrors: true };
  assert.throws(() => queryProvenance(frames, request), /partial/);
  const observed = coverageObservation({ startedAtMs: 100, endedAtMs: 200,
    before: { at: new Date(110).toISOString() }, after: { at: new Date(190).toISOString() },
    query: { startedAtMs: 120, endedAtMs: 180, clientRequestId: request, response: frames } });
  assert.ok(observed.telemetryErrors?.some(error => error.includes('partial')));
  assert.equal(observed.provenance, undefined);
});

const root = 'runs/fixture/table';
function visibility(version: number, start: number) {
  const prefix = `${root}/_delta_log/`;
  const body = `<EnumerationResults><Prefix>${prefix}</Prefix><Blobs>${
    Array.from({ length: version + 1 }, (_, index) => `<Blob><Name>${prefix}${String(index).padStart(20, '0')}.json</Name></Blob>`).join('')
  }</Blobs><NextMarker/></EnumerationResults>`;
  const receipt = { status: 200, bodyBase64: Buffer.from(body).toString('base64'), headers: { 'x-ms-request-id': `request-${start}` } };
  return { startedAtMs: start, endedAtMs: start + 1, ...parseDeltaVisibility(receipt, prefix), receipt };
}
function fixture() {
  const observations = [visibility(1, 970_000), visibility(2, 980_000), visibility(2, 1_099_000), visibility(3, 1_100_000)];
  const source = { measurementStartMs: 1_000_000, measurementEndMs: 1_600_000, warmupMs: 60_000,
    steadyStateStartMs: 1_120_000, writerStoppedAtMs: 1_601_000,
    writerRecords: [2, 3].map((version, index) => ({ version, rows: 10, files: 1, table_id: 'logical-table',
      finished_at_unix_ms: index ? 1_100_000 : 980_000 })),
    sourceVisibility: observations.map(({ receipt: _, ...summary }) => summary),
    source: { delta_version: 3, active_rows: 20, append_only: true,
      commits: [2, 3].map(version => ({ version, rows: 10, files: 1, bytes: 256 })),
      source_reference: { observation: 'table', version: 3, table_root: root,
        active_files: [2, 3].map(version => ({ path: `${version}.parquet`, size: 256, num_records: 10 })),
        active_file_count: 2, active_bytes: 512, row_count: 20,
        provenance: { source_objects: [0, 1, 2, 3].map(version => ({
          name: `${root}/_delta_log/${String(version).padStart(20, '0')}.json`, etag: '"etag"', bytes: 1, sha256: '0'.repeat(64),
        })) } } } };
  return { source, observations };
}

test('the adapter joins exact durable cohorts to contiguous snapshots and raw visibility receipts', () => {
  const { source, observations } = fixture();
  const result = buildCoverageInput(source, [], observations, true);
  assert.deepEqual(result.sourceSnapshots.map(snapshot => [snapshot.version, snapshot.rows, snapshot.files]),
    [[0, 0, 0], [1, 0, 0], [2, 10, 1], [3, 20, 2]]);
  assert.ok(!analyzeCoverage(result).reasons.some(reason => reason.startsWith('SOURCE_FILE_INVENTORY_MISMATCH')));
  assert.deepEqual(result.cohorts.map(cohort => cohort.commitBounds),
    [{ lowerMs: 970_000, upperMs: 980_001 }, { lowerMs: 1_099_000, upperMs: 1_100_001 }]);
  assert.equal(result.clockErrorMs, null);
  assert.equal(result.sourceFinalized, true);
  assert.equal(result.producerStoppedAtMs, source.measurementEndMs);
  source.sourceVisibility[2]!.version = 3;
  assert.throws(() => buildCoverageInput(source, [], observations, true), /raw receipt binding/);
});

test('missing warmup absence, mismatched writer records and incomplete replay cannot be repaired with guesses', () => {
  const { source, observations } = fixture();
  observations.shift(); source.sourceVisibility.shift();
  const input = buildCoverageInput(source, [], observations, true);
  assert.equal(input.cohorts[0]!.commitBounds, undefined);
  assert.ok(analyzeCoverage(input).reasons.includes('COMMIT_BOUNDS_INCOMPLETE'));
  source.writerRecords[1]!.rows = 11;
  assert.throws(() => buildCoverageInput(source, [], observations, true), /matching durable writer record/);
  source.writerRecords[1]!.rows = 10;
  source.source.source_reference.provenance.source_objects.splice(1, 1);
  assert.throws(() => buildCoverageInput(source, [], observations, true), /every cumulative version/);
});

test('resource qualification requires the actual QAP resource, bounded cluster trace and complete metric window', () => {
  const observations = Array.from({ length: 21 }, (_, index) => ({
    startedAtMs: index * 30_000, endedAtMs: index * 30_000 + 1,
    cluster: { location: 'Canada Central', sku: { name: 'Standard_E4ads_v5', capacity: 2 }, properties: {
      state: 'Running', provisioningState: 'Succeeded', optimizedAutoscale: { isEnabled: true, minimum: 2, maximum: 4 },
    } },
    capacity: { Tables: [{ Columns: ['Resource', 'Total', 'Consumed'].map(ColumnName => ({ ColumnName })),
      Rows: [['QueryAcceleration', 4, 4]] }] },
  }));
  const metrics = { response: { value: ['CPU', 'CacheUtilization', 'InstanceCount'].map(name => ({
    name: { value: name }, timeseries: [{ data: Array.from({ length: 11 }, (_, index) => ({
      timeStamp: new Date(index * 60_000).toISOString(), average: name === 'InstanceCount' ? 2 : 1, maximum: 2,
    })) }],
  })) } };
  assert.equal(resourceTrace(observations, 0, 600_000, metrics).complete, true);
  const cost = resourceTrace(observations, 1_000, 599_000, metrics).engineCost;
  assert.equal(cost?.startMs, 60_000);
  assert.equal(cost?.endMs, 540_000);
  assert.equal(cost?.nodeSeconds, 960);
  assert.ok(Math.abs(cost!.estimatedUsd - (1.456 * 8 / 60)) < 1e-12);
  metrics.response.value[2]!.timeseries[0]!.data.splice(4, 1);
  assert.equal(resourceTrace(observations, 1_000, 599_000, metrics).engineCost, null);
  assert.equal(resourceTrace(observations.slice(0, 4), 0, 600_000, metrics).complete, false);
  assert.equal(resourceTrace(observations, 0, 600_000).complete, false);
  observations[0]!.capacity.Tables[0]!.Rows[0]![0] = 'Ingestions';
  assert.equal(resourceTrace(observations, 0, 600_000, metrics).complete, false);
});
