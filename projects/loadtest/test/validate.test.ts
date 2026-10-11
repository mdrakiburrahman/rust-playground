import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mapHostPath } from '../src/main.js';
import { commitSpansCores, parseInspection, signalsReady, validateSignals, type Inspection } from '../src/validate.js';

function fixture(): Inspection {
  const result: Inspection = {
    files: 3,
    schemas: {},
    versions: {},
    active_files: {},
    file_namespaces: {},
    tables: {
      logs: [{ id: 1, body: { str: 'greeting generated' }, trace_id: '1'.repeat(32), span_id: '2'.repeat(16) }],
      log_attrs: [{ parent_id: 1, key: 'run.id', str: 'current' }],
      spans: [{ id: 2, name: 'greeting', trace_id: '1'.repeat(32), span_id: '2'.repeat(16) }],
      span_attrs: [{ parent_id: 2, key: 'run.id', str: 'current' }],
      univariate_metrics: [{ id: 3, name: 'hello_world.greetings' }],
      number_data_points: [{ id: 4, parent_id: 3, int_value: 1 }],
      number_dp_attrs: [{ parent_id: 4, key: 'run.id', str: 'current' }],
      resource_attrs: [{ parent_id: 5, key: 'service.name', str: 'hello-world' }],
      scope_attrs: [],
    },
  };
  for (const [table, rows] of Object.entries(result.tables)) {
    result.versions[table] = 1;
    result.active_files[table] = 1;
    result.file_namespaces[table] = { 'part.parquet': ['session-1'] };
    for (const row of rows) row._otel_join_namespace = 'session-1';
  }
  for (const table of ['logs', 'spans', 'number_data_points']) {
    result.schemas[table] = { fields: [
      { name: 'time', type: 'timestamp' },
      { name: 'time__unix_nanos', type: 'long' },
    ] };
    for (const row of result.tables[table]) {
      row.time = '2026-10-10 00:00:00.000001 +00:00';
      row.time__unix_nanos = '1791590400000001999';
    }
    for (const table of ['logs', 'spans']) {
      for (const row of result.tables[table]) {
        row.resource = { id: 5 };
        row.scope = { id: 6, name: 'hello-world' };
      }
    }
  }
  return result;
}

test('accepts all three signals with joined attributes and correlated IDs', () => {
  validateSignals(fixture(), 'current', 1);
});

test('rejects stale data from another run', () => {
  assert.throws(() => validateSignals(fixture(), 'previous', 1), /logs/u);
});

test('rejects a missing signal', () => {
  const value = fixture();
  delete value.tables.spans;
  assert.throws(() => validateSignals(value, 'current', 1), /spans/u);
});

test('rejects wrong log body and uncorrelated trace IDs', () => {
  const value = fixture();
  value.tables.logs[0].body = { str: 'unrelated' };
  assert.throws(() => validateSignals(value, 'current', 1), /body/u);
  value.tables.logs[0].body = { str: 'greeting generated' };
  value.tables.logs[0].trace_id = '3'.repeat(32);
  assert.throws(() => validateSignals(value, 'current', 1), /correlated/u);
});

test('rejects a counter joined to the wrong metric', () => {
  const value = fixture();
  value.tables.number_data_points[0].parent_id = 99;
  assert.throws(() => validateSignals(value, 'current', 1), /datapoints/u);
});

test('rejects corrupt trace IDs and a counter that never reaches the expected value', () => {
  const value = fixture();
  value.tables.spans[0].span_id = '0'.repeat(16);
  value.tables.logs[0].span_id = '0'.repeat(16);
  assert.throws(() => validateSignals(value, 'current', 1), /Invalid span ID/u);
  value.tables.spans[0].span_id = '2'.repeat(16);
  value.tables.logs[0].span_id = '2'.repeat(16);
  value.tables.number_data_points[0].int_value = 0;
  assert.throws(() => validateSignals(value, 'current', 1), /counter datapoint/u);
});

test('rejects invalid and empty inspection output', () => {
  assert.throws(() => parseInspection({}), /Invalid/u);
  assert.throws(() => parseInspection({ files: 1, schemas: {}, tables: { logs: [null] } }), /Invalid/u);
  const value = fixture();
  value.files = 0;
  assert.throws(() => validateSignals(value, 'current', 1), /No committed Delta/u);
});

// Scenario: restarted writers reuse numeric IDs in different namespaces.
// Guarantees: attribute joins cannot select unrelated rows from another session.
test('does not join colliding numeric IDs across namespaces', () => {
  const value = fixture();
  value.tables.logs[0]._otel_join_namespace = 'session-2';
  assert.throws(() => validateSignals(value, 'current', 1), /logs/u);
});

test('rejects missing nanos and timestamps inconsistent with exact nanos', () => {
  const value = fixture();
  value.tables.logs[0].time__unix_nanos = undefined;
  assert.throws(() => validateSignals(value, 'current', 1), /precision/u);
  value.tables.logs[0].time__unix_nanos = '1791590400000002999';
  assert.throws(() => validateSignals(value, 'current', 1), /disagrees/u);
});

test('maps Docker-outside-Docker paths using the most specific bind', () => {
  assert.equal(mapHostPath('/workspaces/repo/onelake', [
    { Type: 'bind', Source: '/host', Destination: '/workspaces' },
    { Type: 'bind', Source: '/host/project', Destination: '/workspaces/repo' },
  ]), '/host/project/onelake');
  assert.throws(() => mapHostPath('/unmounted', []), /No host bind/u);
});

// Scenario: a commit contains several files, possibly from only one core.
// Guarantees: multi-file existence alone cannot satisfy cross-core batching acceptance.
test('requires different namespaces in the same multi-file commit', () => {
  const value = fixture();
  value.file_namespaces.logs = { 'a.parquet': ['core-1'], 'b.parquet': ['core-1'], 'c.parquet': ['core-2'] };
  assert(!commitSpansCores(value, 'logs', ['a.parquet', 'b.parquet']));
  assert(commitSpansCores(value, 'logs', ['a.parquet', 'c.parquet']));
  assert(!commitSpansCores(value, 'logs', ['c.parquet']));
});

// Scenario: metric datapoints have committed but their parent metric table has not.
// Guarantees: partial Delta visibility is not mistaken for complete three-signal delivery.
test('waits for committed metric parents before declaring signals ready', () => {
  const value = fixture();
  assert(signalsReady(value, 'current', 1));
  value.tables.univariate_metrics = [];
  assert(!signalsReady(value, 'current', 1));
});

// Scenario: a nested resource reference differs from its committed attribute parent ID.
// Guarantees: successful signal counts cannot hide broken resource joins.
test('rejects resource references that do not join attributes', () => {
  const value = fixture();
  value.tables.logs[0].resource = { id: 999 };
  assert.throws(() => validateSignals(value, 'current', 1), /Resource attributes/u);
});
